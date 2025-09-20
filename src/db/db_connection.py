import psycopg2
from src.config.settings import DB_HOST, DB_NAME,DB_PORT,DB_USER,DB_PASSWORD

def get_conn():
    return psycopg2.connect(
        dbname = DB_NAME,
        user = DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
