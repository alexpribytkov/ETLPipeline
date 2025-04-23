# 1. Импортируем нужные библиотеки
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.providers.postgres.hooks.postgres import PostgresHook # Определяет, как подключиться к Postgres. Определили его в connection Airflow, с помощью компоуза
from airflow.providers.postgres.operators.postgres import PostgresOperator # Запустить SQL-запрос
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor # проверяет статус задачи или DAG в другом DAG
import requests #Для запросов к серверу
import xml.etree.ElementTree as ET

# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 60, # задержка перед повторением
    'start_date': days_ago(1)
}

# Тянем по API данные в формате XML и сохраняем их в локальный файл `path` XML 
def extract_data(url, path):
  request = requests.get(url, verify=False)  # Выполняем GET-запрос для получения данных за указанную дату
    # Сохраняем полученные данные (в формате XML) в локальный файл
  with open(path, "w", encoding="utf-8") as tmp_file:
    tmp_file.write(request.text)  # Записываем текст ответа в файл

# 3. Инициализируем DAG
with DAG(
	dag_id="TRY",  # Уникальный ID DAG
	description="Загрузка исторических курсов",
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

    extract_usd = PythonOperator(
        task_id="extract_usd",
        python_callable=extract_data,
        op_kwargs={
            "url": e.path_to_xml(e.start_date_cbr, e.end_date_cbr, e.USD),
            "path": '/opt/airflow/data_lake/data'
            }
        )
    

(
    dag_start
    >> extract_usd
    >> dag_end
)