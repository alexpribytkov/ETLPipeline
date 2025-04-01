# импорт библиотек
import pandas as pd
from io import StringIO
import psycopg2
from sqlalchemy import create_engine
import csv
import json

# Параметры подключения к PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5431"
DB_NAME = "postgres_publicist"

# Создаем подключение c помощью psycopg2
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME, 
    user=DB_USER, 
    password=DB_PASSWORD
)
# Создаем строку подключения SQLAlchemy
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# Создаем курсор psycopg2
cursor = conn.cursor()

# Создаем таблицу №1 users
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
conn.commit() # Необходимо сохранять каждую транзакцию

# Создаем таблицу №2 transactions
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
            zip REAL,
            mcc INTEGER,
            errors VARCHAR(150)             
        );
    """)
conn.commit()

# Создаем таблицу №3 cards
cursor.execute("""               
        CREATE TABLE IF NOT EXISTS cards(
            id SERIAL PRIMARY KEY,
            client_id INTEGER,
            card_brand VARCHAR(50),
            card_type VARCHAR(50),
            card_number BIGINT,
            expires VARCHAR(50),
            cvv SMALLINT,
            has_chip VARCHAR(50),
            num_cards_issued SMALLINT,
            credit_limit MONEY,
            acct_open_date VARCHAR(50),
            year_pin_last_changed SMALLINT,
            card_on_dark_web VARCHAR(50)         
        );              
    """)
conn.commit() # Необходимо сохранить транзакцию

# Создаем таблицу №4 mcc_codes
cursor.execute("""               
        CREATE TABLE IF NOT EXISTS mcc_codes(
            id SERIAL PRIMARY KEY,
            code INTEGER,
            description TEXT
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

# Загрузка данных в таблицу №4 mcc_codes
with open("Datasets/mcc_codes.json", "r", encoding="utf-8") as jsonFile:
    data = json.load(jsonFile) # Преобразование строки JSON в словарь. Парсинг JSON в dict.
df = pd.DataFrame(data.items(),columns=['code', 'description']) # Преобразование словаря в DataFrame.
# Загружаем данные в PostgreSQL с помощью SQLAlchemy
df.to_sql(
    name='mcc_codes',   # Название таблицы
    con=engine,         # Подключение
    if_exists="append", # Режим: "append" (добавить), "replace" (перезаписать), "fail" (ошибка, если таблица существует)
    index=False,        # Не сохранять индексы DataFrame
)

# Загрузка данных в таблицу №1 users. Грузим через copy psycopg2, тк быстрее
# Этот код загружает данные из CSV-файла в таблицу PostgreSQL с помощью эффективного метода массовой вставки через COPY. Разберем каждую часть:
# 1. Чтение CSV в DataFrame
df = pd.read_csv("Datasets/users_data.csv") # Чтение CSV в DataFrame
# 2. Подготовка данных для PostgreSQL
sio = StringIO() # Создание буфера в оперативной памяти (как виртуального файла). StringIO creates a text stream object that behaves like a file but operates in memory.
writer = csv.writer(sio) # Объект для записи данных в CSV-формат 
writer.writerows(df.values) # Запись данных из DataFrame в буфер. df преобразовываем в numpy
sio.seek(0) # Перемещение указателя буфера в начало
# 3. Вставка данных в PostgreSQL через COPY
# COPY — команда PostgreSQL для быстрой массовой загрузки данных.
# FROM STDIN указывает, что данные будут читаться из входного потока (буфера sio).
# WITH CSV — формат данных (CSV).
# conn.commit() фиксирует транзакцию (данные сохраняются в БД)
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

# Загрузка данных в таблицу №2 cards. Грузим через copy psycopg2, тк быстрее
df = pd.read_csv("Datasets/cards_data.csv") 
sio = StringIO()
writer = csv.writer(sio)
writer.writerows(df.values)
sio.seek(0)
with conn.cursor() as c:
    c.copy_expert(
        sql="""
        COPY cards (
            id,
            client_id,
            card_brand,
            card_type,
            card_number,
            expires,
            cvv,
            has_chip,
            num_cards_issued,
            credit_limit,
            acct_open_date,
            year_pin_last_changed,
            card_on_dark_web
        ) FROM STDIN WITH CSV""",
        file=sio
    )
    conn.commit()

# Загрузка данных в таблицу №3 transactions. Грузим через copy psycopg2, тк быстрее
df = pd.read_csv("Datasets/transactions_data.csv")
sio = StringIO()
writer = csv.writer(sio)
writer.writerows(df.values)
sio.seek(0)
with conn.cursor() as c:
    c.copy_expert(
        sql="""
        COPY transactions (
            id,
            date,
            client_id,
            card_id,
            amount,
            use_chip,
            merchant_id,
            merchant_city,
            merchant_state,
            zip,
            mcc,
            errors
        ) FROM STDIN WITH CSV""",
        file=sio
    )
    conn.commit()