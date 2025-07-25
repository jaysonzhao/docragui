import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # S3 配置
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    S3_REGION = os.getenv('S3_REGION', 'none')
    
    # Flask 配置
    SECRET_KEY = os.getenv('SECRET_KEY', 'password')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
