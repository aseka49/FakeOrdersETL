import os
from src.Lake.lake_connection import get_minio_client

minio_client = get_minio_client()

def upload_to_lake(bucket_name, csv_files, local_path):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Бакет '{bucket_name}' создан")
    else:
        print(f"Бакет '{bucket_name}' уже существует")

    for file_name in csv_files:
        filepath = os.path.join(local_path, file_name)
        if os.path.exists(filepath):
            minio_client.fput_object(bucket_name, file_name, filepath)
            print(f"{file_name} загружен в MinIO")
        else:
            print(f"Файл {filepath} не найден!")


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
filepath1 = os.path.join(BASE_DIR, 'data', 'raw', 'users.csv')
filepath2 = os.path.join(BASE_DIR, 'data', 'raw', 'restaurants.csv')
filepath3 = os.path.join(BASE_DIR, 'data', 'raw', 'dishes.csv')


bucket_name = "mybucket"
csv_files = ["users.csv", "restaurants.csv", "dishes.csv"]
local_path = r"/data/raw"

upload_to_lake(bucket_name,csv_files,local_path)