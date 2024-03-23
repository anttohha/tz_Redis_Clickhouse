from clickhouse_driver import Client
import faker
import random
import redis
import json

# Подключение к ClickHouse
clickhouse_client = Client(host='localhost', database='default')

# Создание таблицы
clickhouse_client.execute('''
CREATE TABLE IF NOT EXISTS users (
    username String,
    ipv4 String,
    mac String
) ENGINE = MergeTree() ORDER BY (username, ipv4, mac);
''')

# Подключение к Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Генерация данных
fake = faker.Faker()
for _ in range(1000):
    # Генерация одной записи
    data = {
        'id': fake.uuid4(),
        'ipv4': fake.ipv4(),
        'mac': fake.mac_address()
    }

    # Вставка данных в ClickHouse
    clickhouse_client.execute(
        'INSERT INTO users (username, ipv4, mac) VALUES',
        [(data['id'], data['ipv4'], data['mac'])]
    )

    # Добавление данных в очередь Redis
    redis_client.rpush('task_queue', json.dumps(data))

print("Данные добавлены в ClickHouse и задачи в очередь Redis.")
