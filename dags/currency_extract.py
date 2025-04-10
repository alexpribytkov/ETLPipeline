# 1. Импортируем нужные библиотеки
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago
import pandas as pd

# 2. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}


def load_data_to_db(postgres_conn_id,start_date,end_date,currency):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id) # Создаем подключение
    engine = hook.get_sqlalchemy_engine()
    url=f'https://www.cbr.ru/scripts/XML_dynamic.asp?date_req1={start_date}&date_req2={end_date}&VAL_NM_RQ={currency}'
    df=pd.read_xml(url).rename(columns={'Id': 'Currency'})
    df.to_sql(
    name='exchange',    # Название таблицы
    con=engine,         # Подключение
    if_exists="append", # Режим: "append" (добавить), "replace" (перезаписать), "fail" (ошибка, если таблица существует)
    index=False,        # Не сохранять индексы DataFrame
)

# 3. Инициализируем DAG
with DAG(
	dag_id="Data_cbr",  # Уникальный ID DAG
	description="Создание таблиц и загрузка в них данных",
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

    add_data_USD = PythonOperator(
        task_id="add_usd",
        python_callable=load_data_to_db,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "start_date": '02/03/2001',
            "end_date": '25/03/2025',
            "currency": 'R01235'
            }
        )

(
    dag_start
    >> check_db_connection 
    >> add_data_USD
    >> dag_end
)