# 1. Импортируем нужные библиотеки
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago
import requests #Для запросов к серверу
import xml.etree.ElementTree as ET

# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

# Тянем по API данные в формате XML и сохраняем их в локальный файл `s_file` XML 
def extract_data(url, s_file):
  request = requests.get(url, verify=False)  # Выполняем GET-запрос для получения данных за указанную дату
    # Сохраняем полученные данные (в формате XML) в локальный файл
  with open(s_file, "w", encoding="utf-8") as tmp_file:
    tmp_file.write(request.text)  # Записываем текст ответа в файл

# Создаем подключение c помощью psycopg2, читаем файл и записываем данные в бд с помощью «INSERT» запроса
def load_data(postgres_conn_id, s_file, sql_script):
  hook = PostgresHook(postgres_conn_id=postgres_conn_id) # обозначаем hook (коннектор)
  conn = hook.get_conn() # this returns psycopg2.connect() object
  cursor = conn.cursor() #  Создаем курсор psycopg2 для выполнения запросов 
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
    if record.get('Id') == 'R01235': # Определяем курс
        currency = 'USD'
    elif record.get('Id') == 'R01239':    
        currency = 'EUR'
    elif record.get('Id') == 'R01375':    
        currency = 'CNY'
    currency_data.append((date,rate,currency)) # Добавляем полученные данные в список строк
  # Пакетная вставка
  cursor.executemany(sql_script, currency_data)
  conn.commit()
  conn.close()  # закрыть соединение 

# 3. Инициализируем DAG
with DAG(
	dag_id="Data_cbr_historical",  # Уникальный ID DAG
	description="Загрузка исторических курсов",
	default_args=DEFAULT_ARGS,
	tags=['admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule=None,
  catchup=False,  # Отключить выполнение пропущенных запусков
	max_active_runs=1,  
	max_active_tasks=1
) as dag:

# 4. Создание тАсок
# сначала создаются два EmptyOperator (start_task и end_task), которые не выполняют никаких действий, а служат только для обозначения начала и конца рабочего процесса.   
    dag_start = EmptyOperator(task_id='dag_start')
     
    dag_end = EmptyOperator(task_id='dag_end')

    check_db_connection = PostgresOperator(
        task_id="check_db_connection",
        postgres_conn_id="server_publicist",
        sql="""
            SELECT 1
        """,
        )

    extract_usd = PythonOperator(
        task_id="extract_usd",
        python_callable=extract_data,
        op_kwargs={
            "url": e.path_to_xml(e.start_date_cbr, e.end_date_cbr, e.USD),
            "s_file": e.path_s3(e.S3, 'USD')
            }
        )
    
    load_usd = PythonOperator(
        task_id="load_usd",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "s_file": e.path_s3(e.S3, 'USD'),
            "sql_script": e.data_table_5_currency
            }
        )

    extract_eur = PythonOperator(
        task_id="extract_eur",
        python_callable=extract_data,
        op_kwargs={
            "url": e.path_to_xml(e.start_date_cbr, e.end_date_cbr, e.EUR),
            "s_file": e.path_s3(e.S3, 'EUR')
            }
        )
    
    load_eur = PythonOperator(
        task_id="load_eur",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "s_file": e.path_s3(e.S3, 'EUR'),
            "sql_script": e.data_table_5_currency
            }
        )

    extract_cny = PythonOperator(
        task_id="extract_cny",
        python_callable=extract_data,
        op_kwargs={
            "url": e.path_to_xml(e.start_date_cbr, e.end_date_cbr, e.CNY),
            "s_file": e.path_s3(e.S3, 'CNY')
            }
        )
    
    load_cny = PythonOperator(
        task_id="load_cny",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "s_file": e.path_s3(e.S3, 'CNY'),
            "sql_script": e.data_table_5_currency
            }
        )

(
    dag_start
    >> check_db_connection 
    >> extract_usd
    >> load_usd
    >> extract_eur
    >> load_eur
    >>extract_cny
    >>load_cny
    >> dag_end
)