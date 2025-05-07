# 1. Импортируем нужные библиотеки
import clickhouse_func as cf
import entities as e
from airflow import DAG # Импорт дага
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from clickhouse_driver import Client
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor # проверяет статус задачи или DAG в другом DAG

# Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 60, # задержка перед повторением
    "start_date": days_ago(1)
}

# Создаем подключение к ClickHouse
client = Client(host=cf.host, 
                port=cf.port,
                user=cf.user, 
                password=cf.password)

# вводим функцию для исполнения sql скриптов в clickhouse
def clickhouse_executor(sql_script):
    client.timeout = 3000
    client.execute(sql_script)

# 3. Инициализируем DAG
with DAG(
	dag_id="4.Data_clickhouse",  # Уникальный ID DAG
	description="Миграция данных из postgreSQL в clickhouse",
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

    """wait_for_data = ExternalTaskSensor( # проверяет статус задачи или DAG в другом DAG
        task_id="wait_for_data",
        external_dag_id="3.CRM_data_pgsql"  # ID внешнего DAG
    )"""
     
    check_db_connection = PythonOperator(
        task_id="check_db_connection",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.check_db
            }
        )

    make_db = PythonOperator(
        task_id="make_db",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.make_db
            }
        )

    crm_postgres_replication = PythonOperator(
        task_id="crm_postgres",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.crm_postgres
            }
        )

    crm_datamart = PythonOperator(
        task_id="crm_datamart",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.crm_datamart
            }
        )

    pg_click_migration_crm = PythonOperator(
        task_id="pg_click_migration_crm",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.pg_click_migration_crm
            }
        )

    mcc_codes = PythonOperator(
        task_id="mcc_codes",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.mcc_codes
            }
        )

    pg_click_migration_mcc_codes = PythonOperator(
        task_id="pg_click_migration_mcc_codes",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.pg_click_migration_mcc_codes
            }
        )

    table_exchange = PythonOperator(
        task_id="table_exchange",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.table_exchange
            }
        )

    pg_click_migration_exchange = PythonOperator(
        task_id="pg_click_migration_exchange",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.pg_click_migration_exchange
            }
        )

    drop_na_open = PostgresOperator(
        task_id="drop_na_open",
        postgres_conn_id="server_publicist",
        sql=e.drop_na_1
        )

    drop_na_low = PostgresOperator(
        task_id="drop_na_low",
        postgres_conn_id="server_publicist",
        sql=e.drop_na_2
        )

    drop_na_high = PostgresOperator(
        task_id="drop_na_high",
        postgres_conn_id="server_publicist",
        sql=e.drop_na_3
        )

    drop_na_close = PostgresOperator(
        task_id="drop_na_close",
        postgres_conn_id="server_publicist",
        sql=e.drop_na_4
        )

    drop_na_volume = PostgresOperator(
        task_id="drop_na_volume",
        postgres_conn_id="server_publicist",
        sql=e.drop_na_5
        )

    market_data_ch = PythonOperator(
        task_id="market_data_ch",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.market_data_ch
            }
        )

    pg_click_migration_market_data = PythonOperator(
        task_id="pg_click_migration_market_data",
        python_callable=clickhouse_executor,
        op_kwargs={
            "sql_script": cf.pg_click_migration_market_data
            }
        )

(
    dag_start
    #>> wait_for_data
    >> check_db_connection
    >> make_db
    >> crm_postgres_replication
    >> crm_datamart
    >> pg_click_migration_crm
    >> mcc_codes
    >> pg_click_migration_mcc_codes
    >> table_exchange
    >> pg_click_migration_exchange
    >> drop_na_open
    >> drop_na_low
    >> drop_na_high
    >> drop_na_close
    #>> drop_na_volume
    >> market_data_ch
    >> pg_click_migration_market_data
    >> dag_end
)