# импорт библиотек
import pandas as pd
from io import StringIO
import psycopg2
import csv

df = pd.read_csv("users_data.csv") # чтение файла csv на локальной машине
#
# Параметры подключения к PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5431"
DB_NAME = "postgres_publicist"
# TABLE_NAME = "users"

# Создаем подключение c помощью psycopg2
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME, 
    user=DB_USER, 
    password=DB_PASSWORD
)
# Создаем таблицы c помощью psycopg2
cursor = conn.cursor()
cursor.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            current_age SMALLINT,
            retirement_age SMALLINT,
            birth_year SMALLINT,
            birth_month SMALLINT,
            gender VARCHAR(50),
            address VARCHAR(150),
            latitude REAL,
            longitude REAL,
            per_capita_income MONEY,
            yearly_income MONEY,
            total_debt MONEY,
            credit_score SMALLINT,
            num_credit_cards SMALLINT    
        );
    """)
conn.commit() # Необходимо сохранить транзакцию

# вставка фрейма данных Pandas в PostgreSQL
sio = StringIO() # StringIO creates a text stream object that behaves like a file but operates in memory.
writer = csv.writer(sio) # Создать объект CSV-писателя 
writer.writerows(df.values) # .values return a Numpy representation of the given DataFrame
sio.seek(0)
with conn.cursor() as c:
    c.copy_expert(
        sql="""
        COPY users (
            id,
            current_age,
            retirement_age,
            birth_year,
            birth_month,
            gender,
            address,
            latitude,
            longitude,
            per_capita_income,
            yearly_income,
            total_debt,
            credit_score,
            num_credit_cards
        ) FROM STDIN WITH CSV""",
        file=sio
    )
    conn.commit()

