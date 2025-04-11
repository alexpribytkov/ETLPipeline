#EXTRACT
import requests #Для запросов к серверу
import xml.etree.ElementTree as ET
import psycopg2 

start_date = '01/01/2010'
end_date = '31/10/2019'
CNY = 'R01375' 
USD = 'R01235'
EUR = 'EUR'
url_CNY = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={CNY}'
url_USD = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={USD}'
url_EUR = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={EUR}'
PATH='dags/datasets'

sql_script= "INSERT INTO exchange (date, rate, currency) VALUES (TO_DATE(%s, 'DD/MM/YYYY'), %s, %s)"


# Параметры подключения к PostgreSQL
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5431"
DB_NAME = "postgres_publicist"

# Создаем таблицы c помощью psycopg2
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME, 
    user=DB_USER, 
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл XML
def extract_data(url, s_file):
  """
  Эта функция выгружает данные по валютам, используя GET-запрос,
  и сохраняет результат в локальный файл `s_file`.
  """
  request = requests.get(url, verify=False)  # Выполняем GET-запрос для получения данных за указанную дату
    # Сохраняем полученные данные (в формате XML) в локальный файл
  with open(s_file, "w", encoding="utf-8") as tmp_file:
    tmp_file.write(request.text)  # Записываем текст ответа в файл


#LOAD
def load_data(s_file):
  # Устанавливаем парсер для XML с кодировкой UTF-8
  parser = ET.XMLParser(encoding="utf-8")
  # Чтение XML файла
  tree = ET.parse(s_file, parser=parser)
  # Получение корневого элемента
  root = tree.getroot()
  # Перебор всех валют в XML и извлечение нужных данных
  currency_data = [] # Список для хранения строк данных
  for record in root.findall('Record'):
    date = record.get('Date')#.replace('.', '/') # дата
    rate = record.find('VunitRate').text.replace(',', '.') # Замена запятой на точку, курс за единицу
    match record.get('Id'): # Определяем валюту
      case 'R01235':
        currency = 'USD'
      case 'R01239':
        currency = 'EUR'
      case 'R01375':
        currency = 'CNY'
    currency_data.append((date,rate,currency)) # Добавляем полученные данные в список строк
  # Пакетная вставка
  cursor.executemany(sql_script, currency_data)
  conn.commit()


extract_data(url_CNY,f'{PATH}/currency_CNY')  # Извлечение данных и сохранение их в файл 'currency'
extract_data(url_USD,f'{PATH}/currency_USD')  
extract_data(url_CNY,f'{PATH}/currency_EUR')
load_data(f'{PATH}/currency_CNY')
load_data(f'{PATH}/currency_USD')
load_data(f'{PATH}/currency_EUR')
conn.close() # Закрываем подключение


