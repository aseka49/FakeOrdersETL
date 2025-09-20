import pandas as pd
from psycopg2.extras import execute_values
from src.db.db_connection import get_conn

def load_users(df: pd.DataFrame) -> None:
    if df.empty:
        return

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS clean;
            CREATE TABLE IF NOT EXISTS clean.users (
                id SERIAL PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                city TEXT
            );
            TRUNCATE TABLE clean.users;
        """)
        rows = [
            (row['first_name'], row['last_name'], row['city'])
            for _, row in df.iterrows()
        ]
        execute_values(
            cur,
            "INSERT INTO clean.users (first_name, last_name, city) VALUES %s",
            rows
        )
        conn.commit()


def load_restaurants(df: pd.DataFrame) -> None:
    if df.empty:
        return

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS clean;
            CREATE TABLE IF NOT EXISTS clean.restaurants (
                id SERIAL PRIMARY KEY,
                name TEXT,
                cuisine TEXT,
                city TEXT,
                rating NUMERIC(2,1)
            );
            TRUNCATE TABLE clean.restaurants;
        """)
        rows = [
            (row['name'], row['cuisine'], row['city'], row['rating'])
            for _, row in df.iterrows()
        ]
        execute_values(
            cur,
            "INSERT INTO clean.restaurants (name, cuisine, city, rating) VALUES %s",
            rows
        )
        conn.commit()
