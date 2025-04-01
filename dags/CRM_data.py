# 1. Импортируем нужные библиотеки
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago



# 2. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

# Создаем подключение c помощью psycopg2
def work_with_postgres(postgres_conn_id, sql_script):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()  # this returns psycopg2.connect() object
    cursor = conn.cursor()
    cursor.execute(sql_script)
    conn.commit()
    conn.close() 

# 3. Инициализируем DAG
with DAG(
	dag_id="Data_to_CRM",  # Уникальный ID DAG
	description="Создание таблиц и загрузка в них данных",
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
        sql="""
            SELECT 1
        """,
        )
     
    add_table_users = PythonOperator(
        task_id="add_table_users",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.add_table_1_users
            }
        )
    add_table_transactions = PythonOperator(
        task_id="add_table_transactions",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.add_table_2_transactions
        }
    )

    add_table_cards = PythonOperator(
        task_id="add_table_cards",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.add_table_3_cards 
        }
    )
    
    add_table_mcc_codes = PythonOperator(
        task_id="add_table_mcc",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.add_table_4_mcc_codes 
        }
    )

    er_cards_users = PythonOperator(
        task_id="er_cards_users",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.er_cards_users
        }
    )
    
    er_transactions_users = PythonOperator(
        task_id="er_transactions_users",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.er_transactions_users
        }
    )

    er_transactions_cards = PythonOperator(
        task_id="er_transactions_cards",
        python_callable=work_with_postgres,
        op_kwargs={
            "postgres_conn_id": "server_publicist",
            "sql_script": e.er_transactions_cards
        }
    )

(
    dag_start
    >> check_db_connection 
    >> add_table_users 
    >> add_table_transactions 
    >> add_table_cards 
    >> add_table_mcc_codes 
    >> er_cards_users 
    >> er_transactions_users 
    >> er_transactions_cards 
    >> dag_end
)