# 1. Импортируем нужные библиотеки
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor # проверяет статус задачи или DAG в другом DAG
import pandas as pd
from io import StringIO
import csv
import json

# 2. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 60, # задержка перед повторением
    "start_date": days_ago(1)
}

# Создаем подключение c помощью psycopg2, читаем файл и записываем данные в бд с помощью «COPY» запроса
def load_data(postgres_conn_id, path, sql_script): # в качестве аргументов передаем соединение (зашито в yaml), путь к файлу и sql скрипт из entities.py
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

def load_data_json(postgres_conn_id, path, sql_script):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()  # this returns psycopg2.connect() object
    cursor = conn.cursor() #  Создаем курсор psycopg2 для выполнения запросов
    with open(path, "r", encoding="utf-8") as jsonFile:
        data = json.load(jsonFile) # Преобразование строки JSON в словарь. Парсинг JSON в dict.
    records = [(key, value) for key, value in data.items()] # Преобразовываем данные в список из кортежей [('5812', 'Eating Places and Restaurants'),('5541', 'Service Stations'),]
    cursor.executemany(sql_script,records) # вставляем каждое значение в кортеже в %s, executemany - как итератор проходится по списку и вставляет значения в %s
    conn.commit()
    conn.close()

# 3. Инициализируем DAG
with DAG(
	dag_id="3.CRM_data_pgsql",  # Уникальный ID DAG
	description="Загрузка данных в CRM (бд - postgreSQL)",
	default_args=DEFAULT_ARGS,
	tags=['admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule='@once',
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
        sql="""SELECT 1"""
        )
    
    add_table_users = PostgresOperator(
        task_id="add_table_users",
        postgres_conn_id="server_publicist",
        sql=e.add_table_1_users
        )

    add_table_transactions = PostgresOperator(
        task_id="add_table_transactions",
        postgres_conn_id="server_publicist",
        sql=e.add_table_2_transactions
        )

    add_table_cards = PostgresOperator(
        task_id="add_table_cards",
        postgres_conn_id="server_publicist",
        sql=e.add_table_3_cards
        )
    
    add_table_mcc_codes = PostgresOperator(
        task_id="add_table_mcc_codes",
        postgres_conn_id="server_publicist",
        sql= e.add_table_4_mcc_codes
        )

    add_data_users = PythonOperator(
        task_id="add_data_users",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "path": e.users_data_path,
            "sql_script": e.data_table_1_users
            }
        )    

    add_data_transactions = PythonOperator(
        task_id="add_data_transactions",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "path": e.transactions_data_path,
            "sql_script": e.data_table_2_transactions
            }
        ) 

    add_data_cards = PythonOperator(
        task_id="add_data_cards",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "path": e.cards_data_path,
            "sql_script": e.data_table_3_cards
            }
        ) 
        
    add_data_mcc_codes = PythonOperator(
        task_id="add_data_mcc_codes",
        python_callable=load_data_json,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "path": e.mcc_codes_path,
            "sql_script": e.data_table_4_mcc_codes
            }
        )

    er_cards_users = PostgresOperator(
        task_id="er_cards_users",
        postgres_conn_id="server_publicist",
        sql= e.er_cards_users
        )   

    er_transactions_users = PostgresOperator(
        task_id="er_transactions_users",
        postgres_conn_id="server_publicist",
        sql= e.er_transactions_users
        )   

    er_transactions_cards = PostgresOperator(
        task_id="er_transactions_cards",
        postgres_conn_id="server_publicist",
        sql= e.er_transactions_cards
        )  

(
    dag_start
    >> check_db_connection
    >> add_table_users
    >> add_table_transactions
    >> add_table_cards
    >> add_table_mcc_codes 
    >> add_data_users
    >> add_data_transactions
    >> add_data_cards
    >> add_data_mcc_codes
    >> er_cards_users 
    >> er_transactions_users 
    >> er_transactions_cards 
    >> dag_end
)