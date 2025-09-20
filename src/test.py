import psycopg2

PG_CONFIG = {
    "dbname": "dev_db",
    "user": "dev_user",
    "password": "aseka4901",
    "host": "127.0.0.1",
    "port": 5432
}

try:
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    # Указываем схему clean
    cur.execute("SET search_path TO clean;")

    # Список таблиц
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='clean';
    """)
    tables = cur.fetchall()
    print("Таблицы в clean:", tables)

    # Проверка первых 5 пользователей
    cur.execute("SELECT * FROM users LIMIT 5;")
    users = cur.fetchall()
    print("Первые 5 пользователей:")
    for user in users:
        print(user)

finally:
    if conn:
        conn.close()
