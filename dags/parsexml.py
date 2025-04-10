#EXTRACT
import requests #Для запросов к серверу
import xml.etree.ElementTree as ET

start_date = '01/01/2010'
end_date = '31/10/2019'
CNY = 'R01375' 
USD = 'R01235'
EUR = 'EUR'
url_CNY = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={CNY}'
url_USD = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={USD}'
url_EUR = f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={EUR}'
PATH='dags/datasets'
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
extract_data(url_CNY,f'{PATH}/currency_CNY')  # Извлечение данных и сохранение их в файл 'currency'
extract_data(url_USD,f'{PATH}/currency_USD')  
extract_data(url_CNY,f'{PATH}/currency_EUR')  