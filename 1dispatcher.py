import os
import logging
import redis.asyncio as aioredis
import json
import asyncio
import time
from dotenv import load_dotenv
load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
MAX_WORKER_LOAD = 3
STOROJ_TIME = 100


TASKS_NEW_CHANNEL = "tasks:new"
WORKERS_EVENTS_CHANNEL = "events:workers"



PROCESSING_LIST_PREFIX = "processing"
QUEUE_PREFIX = "queue"
PRIORITY_QUEUES = [
    f"{QUEUE_PREFIX}:p1:returned",    # 1. Высший приоритет (возвращенные задачи)
    f"{QUEUE_PREFIX}:p2:premium",     # 2. Премиум-пользователи
    f"{QUEUE_PREFIX}:p3:normal",      # 3. Обычные пользователи
]

WORKERS_ALL_SET = "workers:all"
WORKERS_AVAILABLE_SET = "workers:available"
WORKERS_LOAD_HASH = "workers:load"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | DISPATCHER | %(message)s",
    handlers=[
        logging.FileHandler("logs/dispatcher.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("dispatcher")



async def tasks_listener(redis: aioredis.Redis):
    "Слушает канал 'tasks:new', читает приоритет и кладёт задачу в нужную очередь"
    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(TASKS_NEW_CHANNEL)
        log.info(f"Диспетчер задач запущен и подписан на канал '{TASKS_NEW_CHANNEL}'.")

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
            if not message:
                continue

            try:
                task_data = json.loads(message["data"])
                priority = task_data.get("priority", 1)

                if priority > 5:
                    target_queue = PRIORITY_QUEUES[1] # queue:p2:premium
                else:
                    target_queue = PRIORITY_QUEUES[2] # queue:p3:normal

                # lpush кладёт задачи в начало списка
                await redis.lpush(target_queue, json.dumps(task_data))
                log.info(f"Новая задача для user_id ( {task_data['user_id']} ) добавлена в очередь '{target_queue}'.")

            except (json.JSONDecodeError, TypeError) as e:
                log.error(f"Ошибка обработки сообщения из '{TASKS_NEW_CHANNEL}': {e}. Сообщение: {message.get('data')}")


            except Exception as e:
                log.error(f"Неизвестная ошибка при добавлении задачи в список дел")


async def reclaim_tasks(redis, worker_id):

    proc = f"{PROCESSING_LIST_PREFIX}:{worker_id}"
    current_tasks = await redis.lrange(proc, 0, -1)

    if current_tasks:
        log.info(f"Актуальный список задач для возврата у воркера '{worker_id}':")
        for i, task_json in enumerate(current_tasks, 1):
            log.info(f"Задача #{i}: {task_json}")
    else:
        log.info(f"Очередь на обработке у воркера '{worker_id}' пуста. Нечего возвращать.")

    log.info(f"Начинаю процесс возврата {len(current_tasks)} задач в главную очередь...")
    while True:
        task_json = await redis.rpop(proc)
        if not task_json:
            break
        await redis.lpush(PRIORITY_QUEUES[0], task_json)
        log.info(f"ВОЗВРАЩЕНИЕ | {worker_id}: вернул задачу в {PRIORITY_QUEUES[0]}")
    log.info(f"Процесс возврата задач для '{worker_id}' завершен.")



async def workers_events_listener(redis: aioredis.Redis):
    "Слушает 'events:workers' и обновляет состояние воркеров в Redis"
    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(WORKERS_EVENTS_CHANNEL)
        log.info(f"Диспетчер воркеров запущен и подписан на канал '{WORKERS_EVENTS_CHANNEL}'.")
        
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
            if not message:
                continue

            try:
                event_data = json.loads(message["data"])
                event = event_data.get("event")
                worker_id = event_data.get("worker_id")

                if not event or not worker_id:
                    continue

                if event == "ready":
                    log.info(f"Воркер {worker_id} сообщил о готовности!")
                    await redis.sadd(WORKERS_ALL_SET, worker_id)
                    await redis.sadd(WORKERS_AVAILABLE_SET, worker_id)
                    await redis.hset(WORKERS_LOAD_HASH, worker_id, 0)
                
                elif event == "heartbeat":
                    now = time.time()
                    await redis.hset("workers:last_hb", worker_id, now)

                    limit_key = f"worker_limits:{worker_id}"
                    today_date = time.strftime('%Y-%m-%d')
                    worker_last_date = await redis.hget(limit_key, "last_date")

                    if worker_last_date and worker_last_date != today_date:
                        log.info(f"Heartbeat: Обнаружена смена дня для воркера {worker_id}. Сбрасываю лимиты.")
                        await redis.hset(limit_key, mapping={
                            "daily_count": 0,
                            "last_date": today_date,
                            "is_exhausted": "0"
                        })
                        await redis.expire(limit_key, 86400)

                    if "pending_tasks" in event_data:
                        real_load = int(event_data["pending_tasks"])

                        await redis.hset(WORKERS_LOAD_HASH, worker_id, real_load)

                        if real_load < MAX_WORKER_LOAD:
                            await redis.sadd(WORKERS_AVAILABLE_SET, worker_id)
                        else:
                            await redis.srem(WORKERS_AVAILABLE_SET, worker_id)

                elif event == "task_received":
                    task_id = event_data.get("task_id")
                    log.info(f"Воркер '{worker_id}' подтвердил получение задачи {task_id}.")
                
                elif event == "task_complete":
                    log.info(f"Воркер '{worker_id}' сообщил о завершении задачи.")
                    
                    # --- НАЧАЛО ИЗМЕНЕНИЙ ---

                    # 1. Получаем исходный JSON задачи из сообщения от воркера
                    completed_task_json = event_data.get("task_json")
                    if completed_task_json:
                        # 2. Формируем имя персонального processing-листа
                        processing_list = f"{PROCESSING_LIST_PREFIX}:{worker_id}"
                        # 3. Удаляем одно (1) вхождение этой задачи из листа
                        await redis.lrem(processing_list, 1, completed_task_json)
                        log.info(f"Задача удалена из '{processing_list}'.")
                    else:
                        log.warning(f"Получено событие task_complete от '{worker_id}' без 'task_json'. Невозможно подтвердить выполнение.")

                    # 4. Уменьшаем счетчик нагрузки (этот код у вас уже был)
                    new_load = await redis.hincrby(WORKERS_LOAD_HASH, worker_id, -1)
                    
                    # 5. Если воркер снова свободен, возвращаем его в пул доступных
                    if new_load < MAX_WORKER_LOAD:
                        await redis.sadd(WORKERS_AVAILABLE_SET, worker_id)
                    
                    log.info(f"Нагрузка воркера '{worker_id}' снижена до {max(0, new_load)}.")

                elif event == "shutdown":
                    log.info(f"Воркер {worker_id} ушёл на перерыв")
                    reason = event_data.get("reason", "unknown")
                    await reclaim_tasks(redis, worker_id)
                    await redis.srem(WORKERS_ALL_SET, worker_id)
                    await redis.srem(WORKERS_AVAILABLE_SET, worker_id)
                    await redis.hdel(WORKERS_LOAD_HASH, worker_id)
                    await redis.hdel("workers:last_hb", worker_id)
                    limit_key = f"worker_limits:{worker_id}"
                    # Уже есть reclaim_tasks и srem/hdel
                    if reason == "daily_limit_exhausted":
                        await redis.hset(limit_key, "is_exhausted", "1")
                        await redis.expire(limit_key, 86400)  # TTL 24h auto-clean
                        # Для лимита: не добавляем обратно, ждём сброса дня
                        log.warning(f"Shutdown {worker_id} из-за daily limit. Set is_exhausted=1, expire 24h.")
                    else:
                        log.info(f"Shutdown {worker_id} по причине '{reason}' (не limit, ready вернёт).")
                        


            except (json.JSONDecodeError, TypeError) as e:
                log.error(f"Ошибка обработки сообщения из '{WORKERS_EVENTS_CHANNEL}'")
            except Exception as e:
                log.error(f"Непредвиденная ошибка: {e}")


async def hb_expiry_watcher(redis):
    async with redis.pubsub() as pubsub:
        await pubsub.subscribe("__keyevent@0__:expired")
        log.info("HB watcher: слушаю истечения ключей")
        while True:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
            if not msg: 
                continue
            key = msg["data"]  # например "hb:worker-2"
            if key.startswith("hb:"):
                worker_id = key.split("hb:", 1)[1]
                log.warning(f"HB EXPIRED | {worker_id} умер внезапно -> срочная рекламация задач")
                await reclaim_tasks(redis, worker_id)
                await redis.srem(WORKERS_ALL_SET, worker_id)
                await redis.srem(WORKERS_AVAILABLE_SET, worker_id)
                await redis.hdel(WORKERS_LOAD_HASH, worker_id)

async def dispatcher_main_loop(redis: aioredis.Redis):
    "Основной цикл: берёт задачу и свободный воркер и соединяет их"
    log.info("Главный цикл диспетчера запущен!")


    try:
        while True:
            task_json = None  # Инициализируем переменную для задачи
            free_worker_id = None # Инициализируем переменную для воркера
            source_queue = None   # Инициализируем переменную для исходной очереди

            try:
                queue_lengths = {}
                for queue in PRIORITY_QUEUES:
                    length = await redis.llen(queue)
                    queue_lengths[queue] = length
                    log.info(f"Количество заявок в очереди: {queue_lengths}")
            except Exception as e:
                log.error(f"Не удалось получить длину очередей: {e}")


            log.info("Ожидаю новую задачу в очередях...")
            source_queue, task_json = await redis.brpop(PRIORITY_QUEUES, timeout=0)
            # source_queue = source_queue_b('utf-8')
            # task_json = task_json_b('utf-8')
            log.info(f"Получена задача из очереди '{source_queue}'. Ищу свободного воркера...")

            awailable_workers = await redis.smembers(WORKERS_AVAILABLE_SET)
            free_worker_id = None
            for worker in awailable_workers:
                limit_key = f"worker_limits:{worker}"
                exhausted = await redis.hget(limit_key, "is_exhausted")
                today_date = time.strftime('%Y-%m-%d')
                last_date = await redis.hget(limit_key, "last_date")

                if exhausted != "1" and last_date == today_date:
                    removed_count = await redis.srem(WORKERS_AVAILABLE_SET, worker)
                    if removed_count > 0:

                        free_worker_id = worker.decode('utf-8') if isinstance(worker, bytes) else worker
                        log.info(f"Выбран свободный воркер: '{free_worker_id}' (не исчерпан).")
                else:
                    await redis.srem(WORKERS_AVAILABLE_SET, worker)
                    log.warning(f"Воркер {worker} исчерпан или неактивен сегодня. Убран из пула.")



            if free_worker_id:
                limit_key = f"worker_limits:{free_worker_id}"
                exhausted = await redis.hget(limit_key, "is_exhausted")
                last_date = await redis.hget(limit_key, "last_date")
                if exhausted == "1" or last_date != today_date:
                    await redis.sadd(WORKERS_AVAILABLE_SET, free_worker_id)
                    log.warning(f"Воркер {free_worker_id} стал исчерпанным/устаревшим во время проверки. Пропускаю.")
                    free_worker_id = None



            if not free_worker_id:


                log.warning("Нет доступных неисчерпанных воркеров. Возвращаю задачу...")                # Начинаем главный цикл заново, ожидая новую задачу через brpop
                await redis.lpush(source_queue, task_json)
                await asyncio.sleep(3)
                continue

            personal_processing_list  = f"{PROCESSING_LIST_PREFIX}:{free_worker_id}"
            load_before = await redis.llen(personal_processing_list)

            log.info(f"Найден свободный воркер: '{free_worker_id}'. Задач в его списке обработки до назначения: {load_before}.")
            
            await redis.lpush(personal_processing_list, task_json)
            load_after = await redis.llen(personal_processing_list)


            task_data = json.loads(task_json)
            personal_channel = f"tasks:for:{free_worker_id}"

            command_for_worker = {
                "cmd": "generate_video",
                "task_id": task_data.get("task_id"),
                "payload": task_data["prompt"],
                "user_id": task_data["user_id"],
                "task_json": task_json  # Это ключ к отказоустойчивости!
            }

            await redis.publish(personal_channel, json.dumps(command_for_worker))
            log.info(f"Задача для user_id ( {task_data['user_id']} ) отправлена воркеру {free_worker_id}.\nЗадач в списке обработки после назначения: {load_after}")
            
            new_load = await redis.hincrby(WORKERS_LOAD_HASH, free_worker_id, 1)
            if new_load < MAX_WORKER_LOAD:
                await redis.sadd(WORKERS_AVAILABLE_SET, free_worker_id)
            else:
                log.info(f"Воркер '{free_worker_id}' достиг максимальной загрузки ({new_load}).")
        
    except Exception as e:
        log.critical(f"Критическая ошибка в главном цикле диспетчера: {e}", exc_info=True)
        if task_json and source_queue:
            await redis.lpush(source_queue, task_json)
        if free_worker_id:
            await redis.sadd(WORKERS_AVAILABLE_SET, free_worker_id)
        await asyncio.sleep(5)


async def main():
    redis_client = aioredis.Redis(host=REDIS_HOST, port = REDIS_PORT, decode_responses=True)

    workers_to_init = ['worker-1', 'worker-2', 'worker-3', 'worker-4']
    today_date = time.strftime('%Y-%m-%d')

    for worker_id in workers_to_init:
        limit_key = f"worker_limits:{worker_id}"
        current_date = await redis_client.hget(limit_key, "last_date")
        # Сбрасываем счётчик если день новый
        if not current_date or current_date != today_date:
            await redis_client.hset(limit_key, mapping={
                "daily_count": 0,
                "last_date": today_date, 
                "is_exhausted": 0
            })
            await redis_client.expire(limit_key, 86400)  # Добавьте TTL всегда
            log.info(f"Сброс лимитов для {worker_id} на новый день.")
        else:
            exhausted = await redis_client.hget(limit_key, "is_exhausted")
            if exhausted == "1":
                log.warning(f"Воркер {worker_id} исчерпан на сегодня. Не добавляем в пул.")
                await redis_client.srem(WORKERS_AVAILABLE_SET, worker_id)


    try:
        await redis_client.ping()
        log.info("Успешно подключились к Redis")
        await redis_client.config_set("notify-keyspace-events", "Ex")
        log.info("Keyspace notifications включены (Ex)")

    except aioredis.ConnectionError as e:
        log.critical(f"Критическая ошибка при подключении к Redis: {e}")
        return
    
    await asyncio.gather(
        tasks_listener(redis_client),
        workers_events_listener(redis_client),
        dispatcher_main_loop(redis_client),
        hb_expiry_watcher(redis_client)

    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Диспетчер остановлен")
