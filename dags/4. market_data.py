## Оперативная инфа 
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
import requests # Для запросов к серверу
import pandas as pd
from io import StringIO
import csv
import json


# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

#EXTRACT
def extract_market_moex(url, path):
  response = requests.get(url) # Получим ответ от сервера
  result = json.loads(response.text)
  data = result['history']['data']   # Преобразование строки JSON в список списков (тянем данные)
  header = result['history']['columns'] # Задаем имена колонок извлекая данные из ответа сервера
  a = len(data) # Узнаем количество полученных строк, если их меньше 100, то мы получили все данные. Лимит JSON 100 записей, всего их около 300
  b = 100
  while a == 100: # если количество строк 100, то переходим на следующую страницу и тянем из нее данные
    url_opt = '?start=' + str(b) 
    url_next_page  = url + url_opt # url к следующей странице
    response = requests.get(url_next_page) 
    result = json.loads(response.text) 
    resp_data = result['history']['data'] # Преобразование строки JSON в список списков
    a = len(resp_data) # узнаем количество полученных строк из этого запроса. Если опять 100 то цикл продолжается
    b = b + 100
    data += resp_data # объединяем списки
  with open(path, "w", newline="",encoding='UTF-8') as file: # сохраняем объединенные списки в формате csv
    writer = csv.writer(file)
    writer.writerow(header)  # Эту строку добавляем к файлу - заголовок
    writer.writerows(data) # Уже сами данные

def load_market_moex(postgres_conn_id, path, sql_script):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id) # обозначаем hook (коннектор)
    conn = hook.get_conn() # this returns psycopg2.connect() object
    cursor = conn.cursor() #  Создаем курсор psycopg2 для выполнения запросов
    df = pd.read_csv(path) # Чтение CSV в DataFrame
    sio = StringIO() # Создание буфера в оперативной памяти (как виртуального файла). StringIO creates a text stream object that behaves like a file but operates in memory.
    writer = csv.writer(sio) # Объект для записи данных в CSV-формат 
    writer.writerows(df.values) # Запись данных из DataFrame в буфер. df преобразовываем в numpy
    sio.seek(0) # Перемещение указателя буфера в начало
    cursor.copy_expert(
        sql=sql_script, # sql скрипт
        file=sio # файл из которого копируются данные
    )
    conn.commit() # сохранить транзакцию
    conn.close() # закрыть соединение 

# 3. Инициализируем DAG
with DAG(
	dag_id="4.Market_data",  # Уникальный ID DAG
	description="Ежедневная загрузка рыночной инфы",
	default_args=DEFAULT_ARGS,
	tags=['admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule='@daily',
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
    
    add_table_market = PostgresOperator(
        task_id="add_table_market",
        postgres_conn_id="server_publicist",
        sql= e.add_table_6_market
        )
    
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_market_moex,
        op_kwargs={
            "url": e.url_moex,
            "path": e.path_to_market_data
            }
        )
    
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_market_moex,
        op_kwargs={
           "postgres_conn_id": "server_publicist",
           "path": e.path_to_market_data,
           "sql_script":e.data_table_6_market
            }
        )

(
    dag_start
    >> check_db_connection
    >> add_table_market
    >> extract_data
    >> load_data
    >> dag_end
)
