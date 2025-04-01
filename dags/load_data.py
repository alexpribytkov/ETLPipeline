import pandas as pd
import entities as e
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago
import csv
from io import StringIO



# 2. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}


def load_data(postgres_conn_id, path_to_csv, sql_script):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()  # this returns psycopg2.connect() object
    cursor = conn.cursor()
    df = pd.read_csv(path_to_csv) # Чтение CSV в DataFrame
    sio = StringIO() # Создание буфера в оперативной памяти (как виртуального файла). StringIO creates a text stream object that behaves like a file but operates in memory.
    writer = csv.writer(sio) # Объект для записи данных в CSV-формат 
    writer.writerows(df.values) # Запись данных из DataFrame в буфер. df преобразовываем в numpy
    sio.seek(0) # Перемещение указателя буфера в начало
    cursor.copy_expert(
        sql=sql_script,
        file=sio
    )
    conn.commit()
    conn.close()

# 3. Инициализируем DAG
with DAG(
	dag_id="Data_to_load",  # Уникальный ID DAG
	description="Загрузка данных",
	default_args=DEFAULT_ARGS,
	tags=['admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule='@once',
    catchup=False,  # Отключить выполнение пропущенных запусков
	max_active_runs=1,
	max_active_tasks=1
) as dag:
    dag_start = EmptyOperator(task_id='dag_start')
     
    dag_end = EmptyOperator(task_id='dag_end')

    check_db_connection = PostgresOperator(
        task_id="check_db_connection",
        postgres_conn_id="server_publicist",
        sql="""
            SELECT 1
        """,
        )
     
    task1 = PythonOperator(
        task_id="add_data_users",
        python_callable=load_data,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "path_to_csv": "../../Datasets/users_data.csv",
            "sql_script": e.data_table_1_users
            }
        )

(
    dag_start
    >> check_db_connection 
    >> task1
    >> dag_end
)