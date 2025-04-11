# 1. Импортируем нужные библиотеки
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

# Тянем по API данные в формате XML 
# Функция для извлечения данных с API Центрального банка и сохранения их в локальный файл XML 
# Эта функция выгружает данные по валютам, используя GET-запрос,и сохраняет результат в локальный файл `s_file`.
def extract_data(url, s_file):
  request = requests.get(url, verify=False)  # Выполняем GET-запрос для получения данных за указанную дату
    # Сохраняем полученные данные (в формате XML) в локальный файл
  with open(s_file, "w", encoding="utf-8") as tmp_file:
    tmp_file.write(request.text)  # Записываем текст ответа в файл

# Создаем подключение c помощью psycopg2 и выполняем конкретный sql скрипт
def work_with_postgres(postgres_conn_id, sql_script): # в качестве аргументов передаем соединение (зашито в yaml) и sql скрипт из entities.py
    hook = PostgresHook(postgres_conn_id=postgres_conn_id) # обозначаем hook (коннектор)
    conn = hook.get_conn() # this returns psycopg2.connect() object
    cursor = conn.cursor() #  Создаем курсор psycopg2 для выполнения запросов
    cursor.execute(sql_script) # используется для выполнения SQL-запросов через курсор
    conn.commit() # сохранить транзакцию
    conn.close()  # закрыть соединение 

# Создаем подключение c помощью psycopg2, читаем файл и записываем данные в бд с помощью «INSERT» запроса
def load_data(s_file):
  # Устанавливаем парсер для XML с кодировкой UTF-8
  parser = ET.XMLParser(encoding="utf-8")
  # Чтение XML файла
  tree = ET.parse(s_file, parser=parser)
  # Получение корневого элемента
  root = tree.getroot()
  # Перебор всех валют в XML и извлечение нужных данных
  currency_data = [] # Список для хранения строк данных
  for record in root.findall('Record'):
    date = record.get('Date')#.replace('.', '/') # дата
    rate = record.find('VunitRate').text.replace(',', '.') # Замена запятой на точку, курс за единицу
    match record.get('Id'): # Определяем валюту
      case 'R01235':
        currency = 'USD'
      case 'R01239':
        currency = 'EUR'
      case 'R01375':
        currency = 'CNY'
    currency_data.append((date,rate,currency)) # Добавляем полученные данные в список строк
  # Пакетная вставка
  cursor.executemany(sql_script, currency_data)
  conn.commit()

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