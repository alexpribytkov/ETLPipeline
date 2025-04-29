import clickhouse_func as cf
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.utils.dates import days_ago
from clickhouse_driver import Client

client = Client(host=cf.host, 
                port=cf.port,
                user=cf.user, 
                password=cf.password)



# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

#2.
def clickhouse_executor(sql_script):
    client.timeout = 3000
    client.execute(sql_script)

# 3. Инициализируем DAG
with DAG(
	dag_id="test_click",  # Уникальный ID DAG
	description="FILL",
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

(
    dag_start
    >> check_db_connection
    >> make_db
    >> dag_end
)
