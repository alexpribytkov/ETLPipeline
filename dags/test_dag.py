# 1. Импортируем нужные библиотеки
from datetime import datetime # для работы с датой
from airflow import DAG # Импорт дага
from airflow.operators.python_operator import PythonOperator # Импорт оператора
from airflow.operators.empty import EmptyOperator #оператор-пустышка, типо pass в python

# 2. Определение функций для тАсок
# Каждая задача (task) — это Python-функция, которую вызывает `PythonOperator`.
def task1():
    print("Выполняется Task 1")
def task2():
    print("Выполняется Task 2")
def task3():
    print("Выполняется Task 3")
    # Можно вызывать внешние скрипты или библиотеки
    # import pandas as pd
    # df = pd.read_csv("data.csv")


# 3. Определяем настройки по умолчанию
default_args = {
    "owner": "admin",
    "retries": 3,  # Количество повторений при ошибке
}

# 4. Инициализируем DAG
dag = DAG(
    dag_id="my_python_dag",  # Уникальный ID DAG
    default_args=default_args,
    description="Пример DAG с PythonOperator",
    start_date=datetime(2025,3,1), # Работать он будет с 01-03-2025
    schedule_interval="@daily",  # Запуск каждый день 
    catchup=False,  # Отключить выполнение пропущенных запусков
)

# 5. Создание тАсок
# сначала создаются два EmptyOperator (start_task и end_task), которые не выполняют никаких действий, а служат только для обозначения начала и конца рабочего процесса.   
start_task = EmptyOperator(task_id='START', dag=dag)    
end_task = EmptyOperator(task_id='END', dag=dag)

task_1 = PythonOperator(
    task_id="task_1", # Имя тАски
    python_callable=task1, # Функция для тАски
    provide_context=True,  # Передача контекста (например, execution_date)
    dag=dag,
)
task_2 = PythonOperator(
    task_id="task_2",
    python_callable=task2,
    dag=dag,
)
task_3 = PythonOperator(
    task_id="task_3",
    python_callable=task3,
    dag=dag,
)

# 6. Определение зависимостей между тАсками
# Вариант 1: task_1 -> task_2 -> task_3
start_task >> task_1 >> task_2 >> task_3 >> end_task
