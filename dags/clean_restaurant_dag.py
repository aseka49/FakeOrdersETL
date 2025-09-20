from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from src.Lake.lake_connection import get_minio_client
from src.etl.extract import extract_from_lake
from src.etl.transform import transform_resraurants_from_lake
from src.etl.load import load_restaurants

BUCKET_NAME = "mybucket"
OBJECT_NAME = "restaurants.csv"

def extract_task(**context):
    minio_client = get_minio_client()
    df = extract_from_lake(minio_client, BUCKET_NAME, OBJECT_NAME)
    return df.to_dict(orient="records")

def transform_task(**context):
    ti = context["ti"]
    df = pd.DataFrame(ti.xcom_pull(task_ids="extract_restaurants_from_lake"))
    df = transform_resraurants_from_lake(df)
    return df.to_dict(orient="records")

def load_task(**context):
    ti = context["ti"]
    df = pd.DataFrame(ti.xcom_pull(task_ids="transform_restaurants_from_lake"))
    load_restaurants(df)

with DAG(
    dag_id="clean_restaurants_from_lake_dag",
    description="ETL: MinIO -> transform -> Postgres",
    start_date=datetime(2025, 9, 18),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract_restaurants_from_lake",
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id="transform_restaurants_from_lake",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load_restaurants_to_postgres",
        python_callable=load_task
    )

    extract >> transform >> load
