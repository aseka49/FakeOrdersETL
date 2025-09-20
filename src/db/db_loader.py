from src.db.db_connection import get_conn
import pandas as pd
from psycopg2 import sql,extras

conn = get_conn()
cursor = conn.cursor()

# Create table


def create_table_if_not_exists(conn, table_name: str, df: pd.DataFrame):
    type_map = []
    for col in df.colums:
        if pd.api.types.is_integer(df[col]):
            sql_type = 'INTEGER'
        elif pd.api.types.is_float(df[col]):
            sql_type = 'NUMERIC'
        elif pd.api.types.is_bool(df[col]):
            sql_type = 'BOOLEAN'
        elif pd.api.types.is_datetime64_dtype(df[col]):
            sql_type = 'TIEMSTAMP'
        else:
            sql_type = 'TEXT'
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXIST {} ({})").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(
                    sql.SQL("{} {}").format(sql.Identifier(col),sql.SQL(t))
                    for col, t in type_map
                )
            )
        )
    conn.commit()


# Load DF to DWH/PostgreSQL

def load_df_to_postgres(conn, df: pd.DataFrame, table_name: str):
    create_table_if_not_exists(conn, table_name, df)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE").format(
                sql.Identifier(table_name)
            )
        )
        extras.execute_values(
            cur,
            sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
                sql.Identifier(table_name),
                sql.SQL(', ').join(map(sql.Identifier, df.columns))
            ).as_string(cur),
            [tuple(row) for row in df.to_numpy()]
        )
    conn.commit()
