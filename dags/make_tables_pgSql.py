# 1. Импортируем нужные библиотеки
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago

# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

# 2. Инициализируем DAG
with DAG(
	dag_id="make_tables_pgSql",  # Уникальный ID DAG
	description="Создание таблиц",
	default_args=DEFAULT_ARGS,
	tags=['admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule=None,
    catchup=False,  # Отключить выполнение пропущенных запусков
	max_active_runs=1,  
	max_active_tasks=1
) as dag:

# 3. Создание тАсок
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
    
    add_table_currency = PostgresOperator(
        task_id="add_table_currency",
        postgres_conn_id="server_publicist",
        sql= e.add_table_5_currency
        ) 
(
    dag_start
    >> check_db_connection 
    >> add_table_users
    >> add_table_transactions
    >> add_table_cards
    >> add_table_mcc_codes
    >> add_table_currency
    >> dag_end
)
