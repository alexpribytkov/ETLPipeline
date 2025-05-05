host='server_clickhouse'
port=9000
user='admin'
password='admin'

check_db = """SELECT 1"""
make_db = """create database pg_db"""

mcc_codes="""CREATE TABLE IF NOT EXISTS mcc_codes(
        id UInt32,
        code UInt32,
        description String
)
ENGINE = MergeTree()  -- Самый популярный движок для аналитических данных
ORDER BY id;  -- Определяет порядок сортировки (не требует уникальности)
"""
insert_mcc_codes="""INSERT INTO mcc_codes 
SELECT id, current_age,birth_year,gender,address,credit_score,num_credit_cards FROM postgresql(    
'server_publicist:5432',
'postgres_publicist',
'users',
'postgres',
'postgres')
"""

create_table_users="""CREATE TABLE IF NOT EXISTS users (
    id Int32,
    current_age Int16,
    birth_year Int16,
    gender String,
    address String,
    credit_score Int16,
    num_credit_cards Int16
)
ENGINE = MergeTree()  -- Самый популярный движок для аналитических данных
ORDER BY id;  -- Определяет порядок сортировки (не требует уникальности)
"""

insert_users="""INSERT INTO users SELECT id, current_age,birth_year,gender,address,credit_score,num_credit_cards FROM postgresql(    
'server_publicist:5432',
'postgres_publicist',
'users',
'postgres',
'postgres')
"""



"""
CREATE TABLE IF NOT EXISTS users (
    id Int32,
    current_age Int16,
    birth_year Int16,
    gender String,
    address String,
    credit_score Int16,
    num_credit_cards Int16
)
ENGINE = MergeTree()
ORDER BY id
"""

"""
INSERT INTO users SELECT id,num_credit_cards, current_age,birth_year,gender,address, yearly_income,total_debt,credit_score FROM postgresql(    
'server_publicist:5432',  -- Адрес PostgreSQL
'postgres_publicist',        -- Имя БД
'users',     -- Имя таблицы
'postgres',           -- Пользователь
'postgres')
"""