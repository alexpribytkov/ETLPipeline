host='server_clickhouse'
port=9000
user='admin'
password='admin'
database='data'

check_db = """SELECT 1"""
make_db = f"create database {database}"

##########
crm_postgres="""CREATE TABLE if not exists data.crm_pg_migration(
    id_client UInt64,
    current_age UInt8,
    birth_year UInt8,
    gender String,
    address String,
    credit_score Int16,
    num_credit_cards UInt8,
    card_brand String,
	card_type String,
	has_chip String,
	num_cards_issued UInt8,
	card_on_dark_web String,
	date date,
	use_chip String,
	merchant_id UInt64,
	merchant_city String,
	merchant_state String,
	mcc UInt16,
	errors String
)
ENGINE = PostgreSQL(
    'server_publicist:5432',  -- Адрес PostgreSQL
    'postgres_publicist',        -- Имя БД
    'datamart',     -- Имя таблицы
    'postgres',           -- Пользователь
    'postgres'        -- Пароль
);"""

crm_datamart ="""CREATE TABLE if not exists data.crm(
    id_client UInt64,
    current_age UInt8,
    birth_year UInt8,
    gender String,
    address String,
    credit_score Int16,
    num_credit_cards UInt8,
    card_brand String,
	card_type String,
	has_chip String,
	num_cards_issued UInt8,
	card_on_dark_web String,
	date date,
	use_chip String,
	merchant_id UInt64,
	merchant_city String,
	merchant_state String,
	mcc UInt16,
	errors String
)
ENGINE = MergeTree()
ORDER BY (date desc)
SETTINGS allow_experimental_reverse_key = 1
"""

pg_click_migration_crm = """INSERT INTO data.crm SELECT * FROM data.crm_pg_migration"""
##########

##########
table_exchange="""create table if not exists data.exchange(
--id UInt32,
date date,
rate Float32,
currency String
)
ENGINE = MergeTree()
ORDER BY (date desc)
SETTINGS allow_experimental_reverse_key = 1"""

pg_click_migration_exchange="""INSERT INTO data.exchange SELECT date, rate, currency  FROM postgresql(    
'server_publicist:5432',  -- Адрес PostgreSQL
'postgres_publicist',        -- Имя БД
'exchange',     -- Имя таблицы
'postgres',           -- Пользователь
'postgres')
WHERE date > '2025-01-01'"""
#WHERE date > (SELECT (max(data.test) FROM stackoverflow.posts)
##########

##########
mcc_codes="""CREATE TABLE IF NOT EXISTS mcc_codes(
        id UInt32,
        code UInt32,
        description String
)
ENGINE = MergeTree()  -- Самый популярный движок для аналитических данных
ORDER BY id;  -- Определяет порядок сортировки (не требует уникальности)
"""

pg_click_migration_mcc_codes="""INSERT INTO mcc_codes 
SELECT id, current_age,birth_year,gender,address,credit_score,num_credit_cards FROM postgresql(    
'server_publicist:5432',
'postgres_publicist',
'users',
'postgres',
'postgres')
"""

##########
"""Прочитать json и csv в кликхаус"""
##########