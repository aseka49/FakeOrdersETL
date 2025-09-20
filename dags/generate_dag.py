from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random
from src.db.db_connection import get_conn

conn = get_conn()

def generate_orders():
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS clean;
            CREATE TABLE IF NOT EXISTS clean.orders (
                order_id SERIAL PRIMARY KEY,
                user_id INT,
                dish_id INT,
                restaurant_id INT,
                quantity INT,
                price NUMERIC,
                order_time TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        cur.execute("SELECT id FROM clean.users;")
        users = [row[0] for row in cur.fetchall()]

        cur.execute("SELECT id, restaurant_id, price FROM clean.dishes;")
        dishes = cur.fetchall()

        if not users or not dishes:
            return ("Нет данных для генерации заказов")


        for _ in range(150):
            user_id = random.choice(users)
            dish_id, restaurant_id, price = random.choice(dishes)
            quantity = random.randint(1, 5)
            cur.execute("""
                INSERT INTO clean.orders (user_id, dish_id, restaurant_id, quantity, price, order_time)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (user_id, dish_id, restaurant_id, quantity, price * quantity))

        conn.commit()

    except Exception as e:
        return ("Ошибка:", e)
    finally:
        if conn:
            conn.close()

with DAG(
    dag_id="generate_orders_dag",
    start_date=datetime(2025, 9, 21),
    schedule_interval=None,
    catchup=False
) as dag:

    generate_task = PythonOperator(
        task_id="generate_orders_task",
        python_callable=generate_orders
    )
