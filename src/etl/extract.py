import pandas as pd
from io import BytesIO
from minio.error import S3Error


def extract_from_lake(minio_client, bucket_name: str, object_name: str) -> pd.DataFrame:
    try:
        response = minio_client.get_object(bucket_name, object_name)
        try:
            df = pd.read_csv(BytesIO(response.read()))
        finally:
            response.close()
            response.release_conn()
        return df
    except S3Error as e:
        raise RuntimeError(f"Can't download {object_name} from {bucket_name}: {e}")
