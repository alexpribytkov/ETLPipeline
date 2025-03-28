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
cursor.execute("""
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

cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions(
            id SERIAL PRIMARY KEY,
            date TIMESTAMP,
            client_id INTEGER,
            card_id INTEGER,
            amount MONEY,
            use_chip VARCHAR(150),
            merchant_id INTEGER,
            merchant_city VARCHAR(150),
            merchant_state VARCHAR(50),
            zip INTEGER,
            mcc INTEGER,
            errors VARCHAR(150)             
        );
    """)
conn.commit() # Необходимо сохранить транзакцию

cursor.execute("""               
        CREATE TABLE IF NOT EXISTS cards(
            id SERIAL PRIMARY KEY,
            client_id INTEGER,
            card_brand VARCHAR(50),
            card_type VARCHAR(50),
            card_number INTEGER,
            expires DATE,
            cvv SMALLINT,
            has_chip VARCHAR(50),
            num_cards_issued SMALLINT,
            credit_limit MONEY,
            acct_open_date DATE,
            year_pin_last_changed SMALLINT,
            card_on_dark_web VARCHAR(50)         
        );              
    """)
conn.commit() # Необходимо сохранить транзакцию

# добавляем связи
cursor.execute("""ALTER TABLE cards ADD FOREIGN KEY (client_id) REFERENCES users (id);""")
conn.commit()
cursor.execute("""ALTER TABLE transactions ADD FOREIGN KEY (client_id) REFERENCES users (id);""")
conn.commit() 
cursor.execute("""ALTER TABLE transactions ADD FOREIGN KEY (card_id) REFERENCES cards (id);""")  
conn.commit()

conn.close()