## Оперативная инфа 
import entities as e # Сюда запишем наши функции
from airflow import DAG # Импорт дага
from airflow.operators.python import PythonOperator # Позволяет выполнять функции на языке Python
from airflow.operators.empty import EmptyOperator # Оператор-пустышка, типо pass в python
from airflow.utils.dates import days_ago
import requests # Для запросов к серверу
import json # Для обработки ответов сервера
import csv


# 1. Определяем настройки по умолчанию
DEFAULT_ARGS = {
    "owner": "admin",
    "retries": 2,  # Количество повторений при ошибке, которые должны быть выполнены перед failing the task
    "retry_delay": 600, # задержка перед повторением
    'start_date': days_ago(1)
}

#EXTRACT
def extract_market_moex(url):
  response = requests.get(url) # Получим ответ от сервера
  result = json.loads(response.text)
  data = result['history']['data']   # Преобразование строки JSON в список списков (тянем данные)
  header = result['history']['columns'] # Задаем имена колонок извлекая данные из ответа сервера
  a = len(data) # Узнаем количество полученных строк, если их меньше 100, то мы получили все данные. Лимит JSON 100 записей, всего их около 300
  b = 100
  while a == 100: # если количество строк 100, то переходим на следубщуб страницу и тянем из нее данные
    url_opt = '?start=' + str(b) 
    url_next_page  = url + url_opt # url к следующей странице
    response = requests.get(url_next_page) 
    result = json.loads(response.text) 
    resp_data = result['history']['data'] # Преобразование строки JSON в список списков
    a = len(resp_data) # узнаем количество полученных строк из этого запроса. Если опять 100 то цикл продолжается
    b = b + 100
    data += resp_data # объединяем списки
  with open(e.path_to_market_data, "w", newline="",encoding='UTF-8') as file: # сохраняем объединенные списки в формате csv
    writer = csv.writer(file)
    writer.writerow(header)  # Эту строку пропускаем, чтобы не писать заголовок
    writer.writerows(data)

# 3. Инициализируем DAG
with DAG(
	dag_id="Market_data",  # Уникальный ID DAG
	description="Ежедневная загрузка рыночной инфы",
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

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_market_moex,
        op_kwargs={
            "url": 'http://iss.moex.com/iss/history/engines/stock/markets/shares/boards/TQBR/securities.json'
            }
        )

(
    dag_start
    >> extract_data
    >> dag_end
)
