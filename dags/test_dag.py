# 1. Импортируем нужные библиотеки
import utils as u # импотрируем файл с функциями для tasks
from datetime import datetime # для работы с датой
from airflow import DAG # Импорт дага
from airflow.operators.python_operator import PythonOperator # Импорт оператора. позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator #оператор-пустышка, типо pass в python

# 2. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, #задержка перед повторением
    'start_date': datetime(2025, 3, 1) # Работать он будет с 01-03-2025 (1 марта)
}

# 3. Инициализируем DAG
with DAG(
	dag_id="my_python_dag",  # Уникальный ID DAG
	description="Пример DAG с PythonOperator",
	default_args=DEFAULT_ARGS,
	tags=['6', 'admin'], # ТЭГ,  по значению тега можно искать экземпляры DAG
	schedule='@daily',
	catchup=False,  # Отключить выполнение пропущенных запусков
	max_active_runs=1,
	max_active_tasks=1
) as dag:
# 4. Создание тАсок
# сначала создаются два EmptyOperator (start_task и end_task), которые не выполняют никаких действий, а служат только для обозначения начала и конца рабочего процесса.   
	dag_start = EmptyOperator(task_id='dag_start')
	dag_end = EmptyOperator(task_id='dag_end')
	task_1 = PythonOperator(
	    task_id="task_1", # Имя тАски
	    python_callable=u.task1 # Функция для тАски
		)
	task_2 = PythonOperator(
		task_id="task_2",
	    python_callable=u.task2
	    )
	task_3 = PythonOperator(
	    task_id="task_3",
	    python_callable=u.task3
	    )
# 5. Определение зависимостей между тАсками
# Вариант 1: task_1 -> task_2 -> task_3
dag_start>> task_1 >> task_2 >> task_3 >> dag_end
