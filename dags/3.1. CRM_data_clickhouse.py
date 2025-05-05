# 1. Импортируем нужные библиотеки
import clickhouse_func as cf # Сюда запишем наши функции
from airflow import DAG # Импорт дага
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
	dag_id="3.1.CRM_data_clickhouse",  # Уникальный ID DAG
	description="Миграция данных из CRM(бд - postgreSQL) в аналитическую СУБД - clickhouse",
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

    wait_for_data = ExternalTaskSensor( # проверяет статус задачи или DAG в другом DAG
        task_id="wait_for_data",
        external_dag_id="3.CRM_data_pgsql"  # ID внешнего DAG
    )
     
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
    >> wait_for_data
    >> check_db_connection
    >> make_db
    >> dag_end
)