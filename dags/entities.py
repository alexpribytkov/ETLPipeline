from datetime import datetime
# Определение функций для тАсок
# Можем импортировать любые библиотеки

#!!!!!!!!!!CRM - DATA!!!!!!!!!!!
users_data_path = "/opt/airflow/data_lake/users_data.csv"
transactions_data_path = "/opt/airflow/data_lake/transactions_data.csv"
cards_data_path = "/opt/airflow/data_lake/cards_data.csv"
mcc_codes_path ="/opt/airflow/data_lake/mcc_codes.json"

add_table_1_users =  """
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
"""

data_table_1_users = """
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
    ) FROM STDIN WITH CSV
"""

add_table_2_transactions = """
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
"""

data_table_2_transactions = """            
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
    ) FROM STDIN WITH CSV
"""

add_table_3_cards = """
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
"""

data_table_3_cards = """
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
    ) FROM STDIN WITH CSV
"""

add_table_4_mcc_codes = """               
    CREATE TABLE IF NOT EXISTS mcc_codes(
        id SERIAL PRIMARY KEY,
        code INTEGER,
        description TEXT
    );              
"""

data_table_4_mcc_codes = """               
    INSERT INTO mcc_codes(
        code,
        description
    ) VALUES (%s, %s)             
"""

er_cards_users = """ALTER TABLE cards ADD FOREIGN KEY (client_id) REFERENCES users (id)"""

er_transactions_users = """ALTER TABLE transactions ADD FOREIGN KEY (client_id) REFERENCES users (id);"""

er_transactions_cards ="""ALTER TABLE transactions ADD FOREIGN KEY (card_id) REFERENCES cards (id);"""

create_datamart = """CREATE VIEW datamart AS
select 
	u.id as id_client,
    u.current_age,
    u.birth_year,
    u.gender,
    u.address,
    u.credit_score,
    u.num_credit_cards,
    c.card_brand,
	c.card_type,
	c.has_chip,
	c.num_cards_issued,
	c.card_on_dark_web,
	t.date,
	t.use_chip,
	t.merchant_id,
	t.merchant_city,
	t.merchant_state,
	t.zip,
	t.mcc,
	t.errors
from users u 
left join cards c on c.client_id=u.id 
left join transactions t on t.card_id=c.id and t.client_id=u.id"""

#!!!!!!!!!!CURRENCY - DATA!!!!!!!!!!!
start_date_cbr = '01/01/2010'
end_date_cbr = datetime.now().strftime("%d/%m/%Y")
CNY = 'R01375' 
USD = 'R01235'
EUR = 'R01239'
S3='/opt/airflow/data_lake'
def path_to_xml(start_date, end_date, currency):
    url = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={currency}'
    return url 
# url_CNY = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={CNY}'
# url_USD = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={USD}'
# url_EUR = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={EUR}'

def path_s3(path, currency):
    path_to_s3 = f'{path}/{currency}'
    return path_to_s3

add_table_5_currency = """ 
    CREATE TABLE IF NOT EXISTS exchange(
        id SERIAL PRIMARY KEY,
        date DATE,
        rate REAL,
        currency VARCHAR
        )"""

data_table_5_currency = """
    INSERT INTO exchange (
        date, 
        rate, 
        currency
    ) VALUES (TO_DATE(%s, 'DD/MM/YYYY'), %s, %s)"""

#!!!!!!!!!!MARKET - DATA!!!!!!!!!!!
url_moex = 'http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities.json'

def current_dateTime():
    current_date = datetime.now().strftime("%d-%m-%Y") # Определение даты и времени для дальнейшего создания файла
    return current_date

path_to_market_data = f"{S3}/market_info_{current_dateTime()}.csv"

add_table_6_market = """ 
    CREATE TABLE IF NOT EXISTS market_data (
	id 	                      SERIAL PRIMARY KEY,
    BOARDID                   VARCHAR(12),
    TRADEDATE                 DATE,
    SHORTNAME                 VARCHAR(50),
    SECID                     VARCHAR(36),
    NUMTRADES                 INTEGER,
    VALUE                     NUMERIC,
    OPEN                      NUMERIC,
    LOW                       NUMERIC,
    HIGH                      NUMERIC,
    LEGALCLOSEPRICE           NUMERIC,
    WAPRICE                   NUMERIC,
    CLOSE                     NUMERIC,
    VOLUME                    BIGINT,
    MARKETPRICE2              NUMERIC,
    MARKETPRICE3              NUMERIC,
    ADMITTEDQUOTE             NUMERIC,
    MP2VALTRD                 NUMERIC,
    MARKETPRICE3TRADESVALUE   NUMERIC,
    ADMITTEDVALUE             NUMERIC,
    WAVAL                     NUMERIC,
    TRADINGSESSION            INTEGER,
    CURRENCYID                VARCHAR(10),
    TRENDCLSPR                NUMERIC,
    TRADE_SESSION_DATE        DATE
        )"""

data_table_6_market = """ 
    COPY market_data (
                BOARDID,
                TRADEDATE,
                SHORTNAME,
                SECID,
                NUMTRADES,
                VALUE,
                OPEN,
                LOW,
                HIGH,
                LEGALCLOSEPRICE,
                WAPRICE,
                CLOSE,
                VOLUME,
                MARKETPRICE2,
                MARKETPRICE3,
                ADMITTEDQUOTE,
                MP2VALTRD,
                MARKETPRICE3TRADESVALUE,
                ADMITTEDVALUE,
                WAVAL,
                TRADINGSESSION,
                CURRENCYID,
                TRENDCLSPR,
                TRADE_SESSION_DATE
        ) FROM STDIN WITH CSV"""