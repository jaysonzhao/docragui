from flask import Flask, render_template, request, jsonify
import boto3
import os
from werkzeug.utils import secure_filename
from config import Config
import uuid
from datetime import datetime
from botocore.exceptions import ClientError, NoCredentialsError

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
            
            # 重置文件指针到开始位置
            file.seek(0)
            
            # 上传文件到S3
            self.s3_client.upload_fileobj(
                file,
                app.config['S3_BUCKET_NAME'],
                unique_filename,
                ExtraArgs={
                    'ContentType': file.content_type or 'application/octet-stream',
                    'Metadata': {
                        'original_filename': filename,
                        'upload_timestamp': datetime.now().isoformat()
                    }
                }
            )
            
            # 生成文件URL
            file_url = f"http://minio-service.minio.svc.cluster.local:9000/{app.config['S3_BUCKET_NAME']}/docs/{unique_filename}"
            
            # 获取文件大小
            file.seek(0, 2)  # 移动到文件末尾
            file_size = file.tell()
            file.seek(0)  # 重置到开始位置
            
            return {
                'success': True,
                'filename': unique_filename,
                'original_filename': filename,
                'url': file_url,
                'size': file_size,
                'upload_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
            return {
                'success': False,
                'error': f'上传失败: {str(e)}'
            }
    
    def list_recent_files(self, limit=10):
        """获取最近上传的文件列表"""
        if not self.s3_client:
            return {'success': False, 'error': 'S3客户端未初始化'}
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=app.config['S3_BUCKET_NAME'],
                MaxKeys=limit
            )
            
            files = []
            if 'Contents' in response:
                for obj in sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True):
                    files.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                        'url': f"http://minio-service.minio.svc.cluster.local:9000/{app.config['S3_BUCKET_NAME']}/docs/{obj['Key']}"
                    })
            
            return {'success': True, 'files': files}
        except Exception as e:
            return {'success': False, 'error': str(e)}

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
        # 检查文件大小
        file.seek(0, 2)  # 移动到文件末尾
        file_size = file.tell()
        file.seek(0)  # 重置到开始位置
        
        if file_size > app.config['MAX_CONTENT_LENGTH']:
            return jsonify({
                'success': False,
                'error': f'文件大小超过限制 ({app.config["MAX_CONTENT_LENGTH"] // (1024*1024)}MB)'
            })
        
        # 上传文件到S3
        upload_result = s3_uploader.upload_file(file, file.filename)
        
        if upload_result['success']:
            return jsonify({
                'success': True,
                'message': '文件上传成功',
                'data': upload_result
            })
        else:
            return jsonify({
                'success': False,
                'error': upload_result['error']
            })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'处理过程中发生错误: {str(e)}'})

@app.route('/files')
def list_files():
    """获取最近上传的文件列表"""
    result = s3_uploader.list_recent_files()
    return jsonify(result)

@app.route('/health')
def health_check():
    """健康检查端点"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        's3_configured': s3_uploader.s3_client is not None
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
