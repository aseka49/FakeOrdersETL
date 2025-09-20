import os
from dotenv import load_dotenv
from minio import Minio

load_dotenv()


def get_minio_client():
    secure = os.getenv("SECURE", "false").lower() == "true"
    return Minio(
        "minio:9000",
        access_key=os.getenv("ACCESS_KEY"),
        secret_key=os.getenv("SECRET_KEY"),
        secure=secure
    )
