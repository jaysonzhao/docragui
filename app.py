from flask import Flask, render_template, request, jsonify
import boto3
import os
from werkzeug.utils import secure_filename
from config import Config
import uuid
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError
import io

app = Flask(__name__)
app.config.from_object(Config)

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {
    'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif', 'csv', 'json', 'xml', 
    'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'zip', 'rar', 'mp4', 'mp3'
}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

class S3Uploader:
    def __init__(self):
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
                endpoint_url="http://minio-service.minio.svc.cluster.local:9000",
                aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY'],
                region_name=app.config['S3_REGION']
            )
            # 测试连接
            self.s3_client.head_bucket(Bucket=app.config['S3_BUCKET_NAME'])
            print(f"成功连接到MinIO，存储桶: {app.config['S3_BUCKET_NAME']}")
        except Exception as e:
            print(f"S3初始化错误: {str(e)}")
            self.s3_client = None
    
    def upload_file(self, file, filename):
        if not self.s3_client:
            return {
                'success': False,
                'error': 'S3客户端未正确初始化，请检查AWS凭证配置'
            }
        
        try:
            # 生成唯一的文件名
            file_extension = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''
            unique_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}_{secure_filename(filename)}"
            
            # 一次性读取文件内容到内存，避免多次seek操作
            file.seek(0)
            file_content = file.read()
            file_size = len(file_content)
            
            # 创建新的BytesIO对象用于上传
            file_obj = io.BytesIO(file_content)
            
            # 设置上传到docs文件夹下的完整路径
            upload_key = f"docs/{unique_filename}"
            
            print(f"开始上传文件到: {app.config['S3_BUCKET_NAME']}/{upload_key}")
            
            # 上传文件到MinIO/S3的docs文件夹
            self.s3_client.upload_fileobj(
                file_obj,
                app.config['S3_BUCKET_NAME'],
                upload_key,
                ExtraArgs={
                    'ContentType': file.content_type or 'application/octet-stream',
                    'Metadata': {
                        'original_filename': filename,
                        'upload_timestamp': datetime.now().isoformat(),
                        'file_size': str(file_size)
                    }
                }
            )
            
            # 生成文件访问URL
            file_url = f"http://minio-service.minio.svc.cluster.local:9000/{app.config['S3_BUCKET_NAME']}/{upload_key}"
            
            print(f"文件上传成功: {upload_key}")
            
            return {
                'success': True,
                'filename': unique_filename,
                'original_filename': filename,
                'url': file_url,
                'size': file_size,
                'upload_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'upload_key': upload_key,
                'folder': 'docs'
            }
        except NoCredentialsError:
            return {
                'success': False,
                'error': 'AWS凭证未找到或无效'
            }
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                return {
                    'success': False,
                    'error': f'S3存储桶 "{app.config["S3_BUCKET_NAME"]}" 不存在'
                }
            elif error_code == 'AccessDenied':
                return {
                    'success': False,
                    'error': '访问被拒绝，请检查AWS权限配置'
                }
            else:
                return {
                    'success': False,
                    'error': f'AWS错误: {str(e)}'
                }
        except Exception as e:
            print(f"上传错误: {str(e)}")
            return {
                'success': False,
                'error': f'上传失败: {str(e)}'
            }
    
    def list_recent_files(self, limit=10):
        """获取docs文件夹下最近上传的文件列表"""
        if not self.s3_client:
            return {'success': False, 'error': 'S3客户端未初始化'}
        
        try:
            # 只列出docs文件夹下的文件
            response = self.s3_client.list_objects_v2(
                Bucket=app.config['S3_BUCKET_NAME'],
                Prefix='docs/',  # 只获取docs文件夹下的文件
                MaxKeys=limit
            )
            
            files = []
            if 'Contents' in response:
                # 过滤掉文件夹本身，只保留文件
                file_objects = [obj for obj in response['Contents'] if not obj['Key'].endswith('/')]
                
                for obj in sorted(file_objects, key=lambda x: x['LastModified'], reverse=True):
                    # 从完整路径中提取文件名
                    display_name = obj['Key'].replace('docs/', '', 1)
                    
                    files.append({
                        'key': obj['Key'],  # 完整的key路径 (docs/filename)
                        'display_name': display_name,  # 显示用的文件名
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                        'url': f"http://minio-service.minio.svc.cluster.local:9000/{app.config['S3_BUCKET_NAME']}/{obj['Key']}"
                    })
            
            return {'success': True, 'files': files, 'folder': 'docs'}
        except Exception as e:
            print(f"列出文件错误: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def delete_file(self, file_key):
        """删除docs文件夹下的文件"""
        if not self.s3_client:
            return {'success': False, 'error': 'S3客户端未初始化'}
        
        try:
            # 确保文件key以docs/开头
            if not file_key.startswith('docs/'):
                file_key = f"docs/{file_key}"
            
            print(f"删除文件: {file_key}")
            
            self.s3_client.delete_object(
                Bucket=app.config['S3_BUCKET_NAME'],
                Key=file_key
            )
            return {'success': True, 'message': f'文件 {file_key} 删除成功'}
        except Exception as e:
            print(f"删除文件错误: {str(e)}")
            return {'success': False, 'error': f'删除失败: {str(e)}'}
    
    def get_file_info(self, file_key):
        """获取文件详细信息"""
        if not self.s3_client:
            return {'success': False, 'error': 'S3客户端未初始化'}
        
        try:
            # 确保文件key以docs/开头
            if not file_key.startswith('docs/'):
                file_key = f"docs/{file_key}"
            
            response = self.s3_client.head_object(
                Bucket=app.config['S3_BUCKET_NAME'],
                Key=file_key
            )
            
            return {
                'success': True,
                'key': file_key,
                'size': response['ContentLength'],
                'last_modified': response['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'content_type': response['ContentType'],
                'metadata': response.get('Metadata', {}),
                'url': f"http://minio-service.minio.svc.cluster.local:9000/{app.config['S3_BUCKET_NAME']}/{file_key}"
            }
        except Exception as e:
            return {'success': False, 'error': f'获取文件信息失败: {str(e)}'}

# 初始化S3上传器
s3_uploader = S3Uploader()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': '没有选择文件'})
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({'success': False, 'error': '没有选择文件'})
    
    if not allowed_file(file.filename):
        return jsonify({
            'success': False, 
            'error': f'不支持的文件类型。支持的格式: {", ".join(ALLOWED_EXTENSIONS)}'
        })
    
    try:
        # 预先读取文件内容来检查大小，避免多次seek操作
        file.seek(0)
        file_content = file.read()
        file_size = len(file_content)
        
        if file_size > app.config['MAX_CONTENT_LENGTH']:
            return jsonify({
                'success': False,
                'error': f'文件大小超过限制 ({app.config["MAX_CONTENT_LENGTH"] // (1024*1024)}MB)'
            })
        
        # 创建新的文件对象用于上传
        file_for_upload = io.BytesIO(file_content)
        file_for_upload.content_type = file.content_type
        file_for_upload.filename = file.filename
        
        # 上传文件到MinIO/S3的docs文件夹
        upload_result = s3_uploader.upload_file(file_for_upload, file.filename)
        
        if upload_result['success']:
            return jsonify({
                'success': True,
                'message': f'文件成功上传到 docs 文件夹',
                'data': upload_result
            })
        else:
            return jsonify({
                'success': False,
                'error': upload_result['error']
            })
        
    except Exception as e:
        print(f"上传处理错误: {str(e)}")
        return jsonify({'success': False, 'error': f'处理过程中发生错误: {str(e)}'})

@app.route('/files')
def list_files():
    """获取docs文件夹下最近上传的文件列表"""
    result = s3_uploader.list_recent_files()
    return jsonify(result)

@app.route('/delete/<path:file_key>', methods=['DELETE'])
def delete_file(file_key):
    """删除docs文件夹下的文件"""
    result = s3_uploader.delete_file(file_key)
    return jsonify(result)

@app.route('/file-info/<path:file_key>')
def get_file_info(file_key):
    """获取文件详细信息"""
    result = s3_uploader.get_file_info(file_key)
    return jsonify(result)

@app.route('/health')
def health_check():
    """健康检查端点"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        's3_configured': s3_uploader.s3_client is not None,
        'minio_endpoint': "http://minio-service.minio.svc.cluster.local:9000",
        'bucket_name': app.config['S3_BUCKET_NAME'],
        'upload_folder': 'docs'
    })

@app.errorhandler(413)
def too_large(e):
    return jsonify({
        'success': False,
        'error': f'文件大小超过限制 ({app.config["MAX_CONTENT_LENGTH"] // (1024*1024)}MB)'
    }), 413

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
