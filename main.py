import requests
from clickhouse_driver import Client
import redis
import json
from concurrent.futures import ThreadPoolExecutor
import time

# Попытки переподключения
RETRY_COUNT = 5
RETRY_DELAY = 5  # В секундах

# Настройка подключения к Redis
def get_redis_connection():
    for _ in range(RETRY_COUNT):
        try:
            return redis.Redis(host='localhost', port=6379, db=0)
        except redis.RedisError:
            time.sleep(RETRY_DELAY)
    raise Exception("Не удалось подключиться к Redis")

# Функция для поиска username в ClickHouse
def search_user_in_clickhouse(ipv4, mac):
    for _ in range(RETRY_COUNT):
        try:
            clickhouse_client = Client(host='localhost', database='default')
            query = f"SELECT username FROM users WHERE ipv4 = '{ipv4}' AND mac = '{mac}' LIMIT 1"
            result = clickhouse_client.execute(query)
            return result[0][0] if result else None
        except Exception as e:
            print(f"Ошибка при запросе к ClickHouse: {e}")
            time.sleep(RETRY_DELAY)
    return None

# Функция для отправки данных во внешний сервис (Pastebin)
def send_data_to_external_service(data, api_dev_key):
    pastebin_params = {
        'api_dev_key': api_dev_key,
        'api_option': 'paste',
        'api_paste_code': data,
        'api_paste_format': 'json'
    }
    for _ in range(RETRY_COUNT):
        try:
            response = requests.post('https://pastebin.com/api/api_post.php', data=pastebin_params)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Ошибка при отправке данных во внешний сервис: {response.status_code}")
                time.sleep(RETRY_DELAY)
        except requests.RequestException as e:
            print(f"Ошибка связи с внешним сервисом: {e}")
            time.sleep(RETRY_DELAY)
    return None

redis_client = get_redis_connection()

# Функция для обработки одной задачи
def process_task(task_data):
    ipv4, mac = task_data['ipv4'], task_data['mac']

    # Поиск username
    username = search_user_in_clickhouse(ipv4, mac)

    if username:
        print(f"Найден пользователь: {username}")
        # Создание JSON для отправки
        json_data = json.dumps({'username': username, 'ipv4': ipv4, 'mac': mac})

        # Отправка данных во внешний сервис (Pastebin)
        try:
            pastebin_url = send_data_to_external_service(json_data, 'W8qiBc4pvade0VfmMTREPSo7BtfBsvpk')
            print(f"URL результата на Pastebin: {pastebin_url}")

            # Сохранение URL в файл
            with open('result_urls.txt', 'a') as file:
                file.write(pastebin_url + '\n')
        except Exception as e:
            print(e)
    else:
        print("Пользователь не найден")


# Функция для извлечения и обработки задач из очереди
def handle_tasks():
    while True:
        task = redis_client.blpop('task_queue', timeout=5)
        if task:
            # Десериализация данных задачи
            task_data = json.loads(task[1].decode())
            process_task(task_data)


# Количество потоков для параллельной обработки
NUM_THREADS = 5

# Использование пула потоков для обработки задач
with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    futures = [executor.submit(handle_tasks) for _ in range(NUM_THREADS)]
    # Ожидание завершения всех задач
    for future in futures:
        future.result()
