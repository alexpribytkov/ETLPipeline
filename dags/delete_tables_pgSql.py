# 1. Импортируем нужные библиотеки
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
	dag_id="DELETE_tables_pgSql",  # Уникальный ID DAG
	description="Удаление таблиц",
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
    
    delete_table_users = PostgresOperator(
        task_id="delete_table_users",
        postgres_conn_id="server_publicist",
        sql='drop table users'
        )

    delete_table_transactions = PostgresOperator(
        task_id="delete_table_transactions",
        postgres_conn_id="server_publicist",
        sql='drop table transactions '
        )

    delete_table_cards = PostgresOperator(
        task_id="delete_table_cards",
        postgres_conn_id="server_publicist",
        sql='drop table cards'
        )
    
    delete_table_mcc_codes = PostgresOperator(
        task_id="delete_table_mcc_codes",
        postgres_conn_id="server_publicist",
        sql= 'drop table mcc_codes'
        )
    
    delete_table_currency = PostgresOperator(
        task_id="delete_table_currency",
        postgres_conn_id="server_publicist",
        sql= 'drop table exchange'
        )
    
    delete_table_market = PostgresOperator(
        task_id="delete_table_market",
        postgres_conn_id="server_publicist",
        sql= 'drop table market_data'
        )

(
    dag_start
    >> check_db_connection 
    >> delete_table_currency
    >> delete_table_market
    >> delete_table_mcc_codes
    >> delete_table_transactions
    >> delete_table_cards
    >> delete_table_users
    >> dag_end
)
