# Определение функций для тАсок
# Можем импортировать любые библиотеки
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