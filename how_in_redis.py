import redis

# Подключение к Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# Получение длины очереди
queue_length = r.llen('task_queue')
print(f"Длина очереди 'task_queue': {queue_length}")

# Получение элементов очереди без удаления
tasks = r.lrange('task_queue', 0, -1)
print("Задачи в очереди 'task_queue':")
for task in tasks:
    print(task.decode('utf-8'))
