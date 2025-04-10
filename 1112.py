import pandas as pd
from sqlalchemy import create_engine

# Параметры подключения к PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5431"
DB_NAME = "postgres_publicist"
# Создаем строку подключения SQLAlchemy
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

start_date='02/03/2001'
end_date='25/03/2025'
curr='R01235'
#url=f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={curr}'
url = 'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1=10/01/2022&date_req2=20/01/2022&VAL_NM_RQ=R01235'
df = pd.read_xml(url).rename(columns={'Id': 'Currency'})
df.to_sql(
  name='exchange',    # Название таблицы
  con=engine,         # Подключение
  if_exists="append", # Режим: "append" (добавить), "replace" (перезаписать), "fail" (ошибка, если таблица существует)
  index=False,        # Не сохранять индексы DataFrame
)
