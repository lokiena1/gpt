import asyncio
import json
import os
import time
import logging
import sys
import signal
import functools
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Any
# Используем асинхронную библиотеку для Redis
import redis.asyncio as redis
# import undetected_playwright.async_api as up  # Импортируем саму библиотеку под коротким именем 'up'
from patchright.async_api import async_playwright # А это нам всё ещё нужно для запуска
from dotenv import load_dotenv
load_dotenv()



REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WORKER_ID = "worker-1"
WORKER_PATH = "logs/worker1.log"
ADMIN_COMMANDS_CHANNEL = "admin:commands"
MAIL_USER = os.getenv("MAIL_USER_1")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD_1")


# Прокси (пример с auth)
PROXY_SERVER = "http://138.219.175.18:8000"
PROXY_USER = "yp5Spy"
PROXY_PASS = "SaPVgG"

TARGET_URL = "https://sora.chatgpt.com/drafts"
# MAIL_USER = "maks1moval@yandex.com"
# MAIL_PASSWORD = "ddj3hegrdui1"
# MAIL_USER_2= "passiveerica@tiffincrane.com"
# MAIL_PASSWORD_2="pahfgty3ehdn"
# m[Ri%O^.<c

EVENTS_CHANNEL  = "events:workers"
# OUT_CHANNEL = "selenium_to_telegram_channel"
CODE_SUB_CHANNEL = "verification_codes_from_mail"

RETURN_QUEUE = "queue:p1:returned"
PENDING_TASKS = []
PROCESSED_VIDEO_IDS = set()

FICTIVE_ID = 0000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    # format="%(asctime)s | %(message)s",

    handlers=[
        logging.FileHandler(WORKER_PATH, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("aiogram-redis-bot")

class GracefulShutdown(Exception):
    pass

@dataclass
class RuntimeState:
    r: Any = None
    hb_task: asyncio.Task | None = None
    monitoring_task: asyncio.Task | None = None
    admin_listener_task: asyncio.Task | None = None
    context: Any = None
    browser: Any = None
    shutting_down: bool = False

RUNTIME = RuntimeState()


async def save_debug_snapshot(page, prefix="error"):
    try:
        log.info(">>> Запущена функция скриншотов")
        html_dump_dir = "saves_html"
        screenshot_dir = "saves_screenshot"
        os.makedirs(html_dump_dir, exist_ok=True)
        os.makedirs(screenshot_dir, exist_ok=True)

        timestamp = int(time.time())

        html_path = os.path.join(html_dump_dir, f"{prefix}_{timestamp}.html")
        screenshot_path = os.path.join(screenshot_dir, f"{prefix}_{timestamp}.png")

        content = await page.content()
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(content)

        await page.screenshot(path=screenshot_path)
        log.info(f"СКРИНШОТЫ |\nСнимок состояния сохранён: {html_path}, {screenshot_path}")
        
        return html_path, screenshot_path
    
    except Exception as e:
        log.error(f"СКРИНШОТЫ |\nОшибка при сохранении скрина и кода страницы: {e}")
        return None, None





async def perform_login(page):
    log_message = {"worker": WORKER_ID, "step": "login", "status": "pending"}
    log.info("АУТЕНТИФИКАЦИЯ |\nНачинаю процесс аутентификации...")

    async def handle_chatgpt_page():
        try:
            if "ChatGPT" in await page.title():
                log.info("АУТЕНТИФИКАЦИЯ | Обнаружена промежуточная страница ChatGPT. Ищу вторую кнопку 'Log in'.")
                chatgpt_login_button = page.locator('button[data-testid="login-button"]')
                if await chatgpt_login_button.is_visible(timeout=10000):
                    await chatgpt_login_button.click()
                    log.info("АУТЕНТИФИКАЦИЯ | Вторая кнопка 'Log in' на странице ChatGPT нажата.")
                    await page.wait_for_load_state('networkidle', timeout=30000)
        except Exception as e:
            log.error("АУТЕНТИФИКАЦИЯ | Промежуточная страница ChatGPT не обработана или отсутствовала.")


    try:
        try:
            log.info("АУТЕНТИФИКАЦИЯ |\nИщу первую кнопку 'Log in' на сайте Sora...")
            login_prompt_button = page.locator('button:has-text("Log in"), button:has-text("Войти")').first
            if await login_prompt_button.is_visible(timeout=30000):
                await login_prompt_button.click()
                log.info("АУТЕНТИФИКАЦИЯ | Первая кнопка 'Log in' нажата.")
                await page.wait_for_timeout(10000)
        except Exception as e:
            log.error("АУТЕНТИФИКАЦИЯ |\nОшибка при поиске кнопки входа на сайте сора: {e}")
            await save_debug_snapshot(page, prefix="error_login")

        await handle_chatgpt_page()

        # --- Шаг 3: Ввод email с обработкой альтернативного сценария ---
        try:        
            # 2. Вводим email. Ищем поле по разным признакам.
            email_input = page.locator('input[name="email"], input[type="email"], #email').first
            log.info("Ожидаю появления поля для ввода email (до 80 секунд)...")
            await email_input.wait_for(state='visible', timeout=80000)

            await email_input.fill(MAIL_USER)
        except Exception as e:
            log.error(f"АУТЕНТИФИКАЦИЯ |\nОшибка в вставке email на сайте GPT: {e}")
            await save_debug_snapshot(page, prefix="error_login")

            try:
                container = page.locator('div:has-text("Log in to create and explore") + div:has(button:has-text("Log in"))')
                # alt_login_text = page.locator('div:has-text("Log in to create and explore")')
                if await container.is_visible(timeout=5000):
                    log.info("АУТЕНТИФИКАЦИЯ |\nОбнаружен альтернативный вариант страницы входа. Ищу кнопку 'Log in'...")

                    alt_login_button = container.get_by_role("button", name="Log in")

                    log.info("АУТЕНТИФИКАЦИЯ |\nНайдена контейнер альтернативный 'Log in', нажимаю...")
                    await alt_login_button.click()
                    await page.wait_for_timeout(5000)
                    log.info("АУТЕНТИФИКАЦИЯ |\nАльтернативная кнопка нажата. Повторяю попытку ввода email.")
                    await save_debug_snapshot(page, prefix="alt_login_clicked")

                    await handle_chatgpt_page()
                    await save_debug_snapshot(page, prefix="info_login")
                    
                    try:
                        log.info("АУТЕНТИФИКАЦИЯ |\nИщу поле для ввода email...")
                        email_input = page.locator('input[name="email"], input[type="email"], #email').first
                        log.info("Ожидаю появления поля для ввода email (до 60 секунд)...")   
                        await email_input.wait_for(state='visible', timeout=60000)   

                        await email_input.fill(MAIL_USER)
                        log.info("нашли кнопку email и вставили туда email")
                    except Exception as e:
                        log.warning(f"На странице всё равно не нашли email: {e}")
                        await save_debug_snapshot(page, prefix="error_login")

                else:
                    raise Exception("Альтернативный сценарий входа не обнаружен.")
            except Exception as e:
                log.error(f"АУТЕНТИФИКАЦИЯ | Ошибка в альтернативном сценарии входа: {e}")
                await save_debug_snapshot(page, prefix="error_login")
                return {"status": "error", "details": str(e)}

        # --- Шаг 4: Ввод пароля ---
        try:
            # Нажимаем кнопку "Продолжить" (Continue)
            await page.locator('button[type="submit"]').click()
            log.info("АУТЕНТИФИКАЦИЯ |\nEmail введён, ожидаю поле для пароля...")

            password_input = page.locator('input[type="password"]')
            await password_input.wait_for(timeout=30000)
            await password_input.fill(MAIL_PASSWORD)
            log.info("АУТЕНТИФИКАЦИЯ |\nПароль введён!")
        except Exception as e:
            log.error(f"АУТЕНТИФИКАЦИЯ |\nОшибка в вводе пароля на сайте GPT: {e}")
            log_message["status"] = "error"
            log_message["details"] = str(e)
            await save_debug_snapshot(page, prefix="error_login")
            return log_message

        # --- Шаг 5: Финальное нажатие и ожидание смены URL ---
        try:
            log.info("АУТЕНТИФИКАЦИЯ |\nИщу финальную кнопку 'Continue' для входа...")    
            continue_button = page.get_by_role("button", name="Continue", exact=True)

            await continue_button.wait_for(state='visible', timeout=35000)

            current_login_url = page.url # Запоминаем текущий URL страницы входа
            log.info(f"АУТЕНТИФИКАЦИЯ |\nНажимаю финальную кнопку 'Continue'. Текущий URL: {current_login_url}")
            await continue_button.click()

            log.info("АУТЕНТИФИКАЦИЯ |\nОжидаю смены URL после входа...")
            
            await page.wait_for_url(lambda url: url != current_login_url, timeout=15000)
            log.info(f"АУТЕНТИФИКАЦИЯ |\nВход выполнен. Новый URL: {page.url}")
            log_message["status"] = "success" # Устанавливаем успех ТОЛЬКО ЗДЕСЬ
            await save_debug_snapshot(page, prefix="success")

        except Exception as e:
            log.error(f"АУТЕНТИФИКАЦИЯ |\nОшибка в нажатии кнопки далее на сайте GPT: {e}")
            await save_debug_snapshot(page, prefix="error_login")
            return 



    except Exception as e:
        log.error(f"АУТЕНТИФИКАЦИЯ |\nошибка на этапе аутентификации: {e}")
        log_message["status"] = "error"
        log_message["details"] = str(e)



    await save_debug_snapshot(page, prefix="error_login")
    return log_message


# Замени эту функцию целиком
async def check_and_close_cameo_popup(page):
    log.info("ГЕНЕРАЦИЯ |\n--- Начало проверки окна 'Add cameo' ---")
    try:
        cameo_popup_selector = 'div.flex.flex-col.px-4.pb-6.pt-4:has-text("Add cameo")'
        popup = page.locator(cameo_popup_selector).first

        if await popup.is_visible(timeout=3000):
            log.info("ГЕНЕРАЦИЯ |\nОкно 'Add cameo' НАЙДЕНО и ВИДИМО. Пытаюсь закрыть.")
            
            # Попытка №1: Нажать Escape
            log.info("ГЕНЕРАЦИЯ |\nПопытка №1: Нажатие клавиши Escape.")
            await page.keyboard.press("Escape")
            await page.wait_for_timeout(1000) # Даем время на реакцию

            # Проверяем, сработало ли
            if not await popup.is_visible(timeout=1000):
                log.info("ГЕНЕРАЦИЯ |\nОкно 'Add cameo' успешно закрыто с помощью Escape.")
                log.info("ГЕНЕРАЦИЯ |\n--- Проверка окна 'Add cameo' завершена ---")
                return True

            # Попытка №2: Клик по центру экрана со смещением
            log.info("ГЕНЕРАЦИЯ |\nПопытка №2: Escape не сработал. Кликаю по центру экрана со смещением (-150px вверх).")
            viewport_size = page.viewport_size
            center_x = viewport_size['width'] / 2
            center_y = viewport_size['height'] / 2 - 150  # Смещаемся вверх от центра

            await page.mouse.click(center_x, center_y)
            await page.wait_for_timeout(1000)

            # Финальная проверка
            if not await popup.is_visible(timeout=1000):
                log.info("ГЕНЕРАЦИЯ |\nОкно 'Add cameo' успешно закрыто кликом мыши.")
                log.info("ГЕНЕРАЦИЯ |\n--- Проверка окна 'Add cameo' завершена ---")
                return True
            else:
                log.warning("ГЕНЕРАЦИЯ |\nНи Escape, ни клик не закрыли окно 'Add cameo'.")
                await save_debug_snapshot(page, prefix="cameo_stuck_error")
        else:
            log.info("ГЕНЕРАЦИЯ |\nОкно 'Add cameo' не стало видимым за 3 секунды. Считаем, что его нет.")

    except Exception as e:
        log.warning(f"ГЕНЕРАЦИЯ |\nВо время проверки/закрытия окна 'Add cameo' произошла ошибка: {e}")

    log.info("ГЕНЕРАЦИЯ |\n--- Проверка окна 'Add cameo' завершена ---")
    return False



# async def check_for_auth_error(page):
#     try:
#         error_selector = 'div:has-text("Your authentication token has been invalidated")'
#         error_element = page.locator(error_selector).first

#         if await error_element.is_visible(timeout=2000):
#             log.error("ГЕНЕРАЦИЯ |\nТокен аутентификации недействителен. Требуется перезапуск.")
#             return True

        
#     except Exception as e:
#         log.error(f"ГЕНЕРАЦИЯ | Ошибка при проверке лимита: {e} (игнорируем, продолжаем).")

    
#     return False


# async def check_for_daily_limit_error(page):
#     try:
#         limit_selector = 'div:has-text("You\'ve already generated 30 videos in the last day")'
#         limit_element = page.locator(limit_selector).first()
#         if await hint_element.is_visible(timeout=2000):
#             log.error("ГЕНЕРАЦИЯ |\nПодтверждение лимита: 'Please try again later'. Аккаунт исчерпан.")
#     except Exception as e:
#         pass





async def check_for_errors_after_submit(page):
    error_type = None

    try:
        task_data = locals().get('task_data')
        log.info(f"ГЕНЕРАЦИЯ | Начинаю объединённую проверку ошибок после отправки(лимиты, токен, общие).")
        
        limit_selector = 'div:has-text("You\'ve already generated 30 videos in the last day")'
        limit_element = page.locator(limit_selector).first
        if await limit_element.is_visible(timeout=1000):
            log.error(f"Обнаружена ошибка 30 видео за день (global daily limit)")
            error_type = 'daily_limit'
            await save_debug_snapshot(page, prefix="error_daily_limit")
            await RUNTIME.r.hset(f"worker_limits:{WORKER_ID}", "is_exhausted", "1")
            await RUNTIME.r.expire(f"worker_limits:{WORKER_ID}", 86400)  # TTL 24h
            log.info(f"ГЕНЕРАЦИЯ | Worker {WORKER_ID} exhausted по daily limit. Set is_exhausted=1.")
            
            # Publish shutdown с reason — диспетчер reclaim'нет задачу в p1
            shutdown_event = {
                "worker_id": WORKER_ID,
                "event": "shutdown",
                "reason": "daily_limit_exhausted",
                "ts": time.time(),
                "task_id": task_data.get('task_id') if 'task_data' in locals() else None  # Если в scope
            }
            await RUNTIME.r.publish(EVENTS_CHANNEL, json.dumps(shutdown_event))
            log.warning(f"ГЕНЕРАЦИЯ | Shutdown event published для {WORKER_ID} по limit.")
            return error_type
            


        auth_selector = 'div:has-text("Your authentication token has been invalidated")'
        auth_element = page.locator(auth_selector).first
        if await auth_element.is_visible(timeout=1000):
            log.error(f'Обнаружена ошибка "Невалидный токен"')
            error_type = 'token_invalidated'
            await save_debug_snapshot(page, prefix="error_auth_token_invalidated")
            return error_type

        hint_selector = 'div:has-text("Something went wrong")'
        hint_element = page.locator(hint_selector).first
        if await hint_element.is_visible(timeout=1000):
            log.error(f'Обнаружена ошибка "Повторите попытку"')
            error_type = 'general_error'
            await save_debug_snapshot(page, prefix="error_something_went_wrong")
            return error_type

        log.info("ГЕНЕРАЦИЯ | Все проверки ошибок пройдены успешно. Продолжаем.")
        return None
    except Exception as e:
        log.warning(f"ГЕНЕРАЦИЯ | Ошибка при проверке ошибок после отправки: {e} (игнорируем, считаем успехом).")
        return None



async def handle_error_after_submit(error_type, page, prompt_text, task_id, redis_client, user_id, task_json=None):
    try:
        log.info("Вызываю функцию для проверки ошибок после отправки")
        # error_type = await check_for_errors_after_submit(page)
        matching_task = next((t for t in PENDING_TASKS if t.get('task_id') == task_id), None)
        if matching_task:
            PENDING_TASKS.remove(matching_task)  # Remove dict object
            log.info(f"Task {task_id} removed from PENDING_TASKS на limit.")
                        
        if error_type == "daily_limit":
            today_date = time.strftime('%Y-%m-%d')
            limit_key = f"worker_limits:{WORKER_ID}"

            await redis_client.hincrby(limit_key, "daily_count", 1)
            current_count = int((await redis_client.hget(limit_key, "daily_count")) or 0)
                    
    
            # Lrem from processing list (диспетчер не incrby -1, так как no task_complete)
            processing_key = f"processing:{WORKER_ID}"


            task_json_for_rem = task_json if task_json else json.dumps({
                "task_id": task_id,
                "user_id": user_id,
                "prompt": prompt_text
                })
            await redis_client.lrem(processing_key, 1, task_json_for_rem)  # Убрать to_thread (async redis)
            log.info(f"Task {task_id} lrem from {processing_key} на limit (used {'full' if task_json else 'approx'}).")
                        


            if current_count >= 30:
                await redis_client.hset(limit_key, mapping={"last_date": today_date, "is_exhausted": "1"})
                await redis_client.expire(limit_key, 86400)  # Add expire 24h
                shutdown_event = {
                    "event": "shutdown",
                    "worker_id": WORKER_ID,
                    "reason": "daily_limit_exhausted",
                    "ts": time.time()
                }
                await redis_client.publish("events:workers", json.dumps(shutdown_event))
                log.warning(f"ГЕНЕРАЦИЯ | Аккаунт {WORKER_ID} исчерпан (30/30). Shutdown отправлен. Пауза на 1 час.")
                await asyncio.sleep(3600)  # Пауза, чтобы не спамить
                return False
            else:
                # <30: return task to p1 (no exhausted)

                task_to_return = {
                    "task_id": task_id,
                    "prompt": prompt_text,
                    "user_id": user_id,
                    "priority": 1  # High priority
                }
                await redis_client.lpush("queue:p1:returned", json.dumps(task_to_return))
                log.info(f"ГЕНЕРАЦИЯ | Task {task_id} lpush to p1:returned (<30 limit). Count: {current_count}/30.")
                return False  # Диспетчер найдёт другого worker



        elif error_type in ['general_error', 'token_invalidated']:
            log.error(f"ГЕНЕРАЦИЯ | Фатальная ошибка '{error_type}'. Перезапуск воркера.")
            await graceful_shutdown("fatal during submit_prompt_and_generate", redis_client, RUNTIME.hb_task, RUNTIME.monitoring_task, RUNTIME.admin_listener_task, RUNTIME.context, RUNTIME.browser)
            raise GracefulShutdown()
        else:

            await save_debug_snapshot(page, prefix="success_submit_click")
            log.info("ГЕНЕРАЦИЯ | Промт успешно отправлен, ошибок нет.")
            return True

    except Exception as e:
        log.error(f"Ошибка при проверке браузера на ошибки: {e}")


async def video_monitoring_loop(page, redis_client):
    log.info(">>> Запущен фоновый цикл мониторинга видео.")
    now = time.time()
    while True:
        try:
            now = time.time()

            for task in PENDING_TASKS:
                if now - task.get('start_time') > 160 and not task.get('timed_out'):
                    log.warning(f"ОБХОД БАГА | Задача для user_id ( {task['user_id']} ) зависла (> 2,6 мин). Отправляю фиктивный промпт.")
                    fake_task_id = f"fake_{uuid.uuid4().hex[:8]}"
                    await submit_prompt_and_generate(page, FICTIVE_ID, fake_task_id)
                    task["timed_out"] = True
                    break


            all_divs = await page.locator('div[data-index]').all()
            target_divs = [div for div in all_divs if int(await div.get_attribute('data-index')) in range(5)]

            if target_divs:
                log.info(f"МОНИТОРИНГ | Из них для проверки выбрано (индексы 0-4): {len(target_divs)}")

            for i, div in enumerate(target_divs):
                video_id = None
                new_page = None

                data_index = await div.get_attribute('data-index')
                log.info(f"\n--- Проверка видео #{i+1} (data-index: {data_index}) ---")
                try:
                    await save_debug_snapshot(page, prefix="violation_check")  # Для отладки
                    violation_text_element = div.locator('div:has-text("This content may violate")')
                    if await violation_text_element.is_visible(timeout=3000):
                        log.error(f"ОБНАРУЖЕНИЕ НАРУШЕНИЯ | Обнаружен блок с нарушением правил на data-index={data_index}.")

                        prompt_element = div.locator('div.text-pretty')
                        if await prompt_element.count() > 0:
                            violating_prompt = await prompt_element.inner_text()

                            matching_task = next((task for task in PENDING_TASKS if task["prompt"] == violating_prompt), None)

                            if matching_task:
                                log.error(f"ОБНАРУЖЕНИЕ НАРУШЕНИЯ | Найдена задача для user_id ( {matching_task['user_id']} ).")
                            
                                failure_result = {
                                    "event": "task_failed_violation",
                                    "user_id": matching_task['user_id'],
                                    "prompt": violating_prompt
                                }

                                await redis_client.publish("results:to_bot", json.dumps(failure_result))
                                log.info("ОБНАРУЖЕНИЕ НАРУШЕНИЯ | Событие о сбое отправлено Боту.")
                                PENDING_TASKS.remove(matching_task)

                                completion_for_dispatcher = {
                                    "event": "task_complete",
                                    "worker_id": WORKER_ID, 
                                    "task_json": matching_task["task_json"]
                                }
                                await redis_client.publish(EVENTS_CHANNEL, json.dumps(completion_for_dispatcher))
                                log.info("ОБНАРУЖЕНИЕ НАРУШЕНИЯ | Подтверждение о завершении отправлено Диспетчеру.")

                                video_id_from_link = await div.locator('a').first.get_attribute('href')
                                if video_id_from_link:
                                    PROCESSED_VIDEO_IDS.add(video_id_from_link)

                                continue

                    error_div = div.locator('text="Something went wrong"').first
                    if await error_div.is_visible(timeout=500):
                        log.warning(f"ГЕНЕРАЦИЯ | Обнаружен блок с ошибкой на позиции data-index={i}. Пытаюсь нажать Retry.")
                        retry_button = div.locator('button:has-text("Retry")').first
                        await retry_button.click()

                        dialog = page.locator('div[role="dialog"][data-state="open"]').first
                        await dialog.wait_for(state="visible", timeout=10000)
                        log.info("ГЕНЕРАЦИЯ | Появился диалог Retry. Нажимаю кнопку отправки.")
                        
                        try:
                            submit_button_in_dialog = dialog.locator('button.bg-token-bg-inverse').first
                            await submit_button_in_dialog.click(force=True)
                            log.info("ГЕНЕРАЦИЯ | Мы нажали кнопку отправки промта на сайт!")
                            await save_debug_snapshot(page, prefix="success_close_window")
                            await dialog.wait_for(state="hidden", timeout=10000)
                            log.info("ГЕНЕРАЦИЯ | Диалог Retry закрыт. Пропускаю эту итерацию.")

                        except Exception as e:
                            log.error(f"Ошибка в переотправке промта из окна Retry: {e}")
                            await save_debug_snapshot(page, prefix="error_close_window")

                        log.info("ГЕНЕРАЦИЯ | Возвращаемся на целевую страницу /drafts после обработки ошибки.")
                        await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
                        # Прерываем текущий цикл for, чтобы начать сканирование заново с чистой страницы
                        break


                    link_element = div.locator('a').first
                    if await link_element.count() == 0:
                        continue

                    video_id = await link_element.get_attribute('href')
                    if not video_id or video_id in PROCESSED_VIDEO_IDS:
                        continue
                    
                    spinner = div.locator('svg circle.origin-center')
                    if await spinner.count() > 0:
                        continue

                    
                    log.info(f"ГЕНЕРАЦИЯ |\nОбнаружено новое готовое видео: {video_id}")
                    base_url = "https://sora.chatgpt.com"
                    full_video_page_url = f"{base_url}{video_id}"

                    context = page.context
                    new_page = await context.new_page()
                    log.info("ГЕНЕРАЦИЯ |\nСоздали новую страницу для перехода")

                    try:
                        await new_page.goto(full_video_page_url, wait_until="domcontentloaded", timeout=20000)
                        log.info(f"НАВИГАЦИЯ |\nУспешно перешел на страницу видео {video_id}.")

                        
                        text_selector = 'div.flex.justify-between.gap-3 > div'
                        text_element = new_page.locator(text_selector).first
                        await text_element.wait_for(state="visible", timeout=10000)

                        actual_prompt = await text_element.inner_text()
                        log.info(f'ГЕНЕРАЦИЯ |\nНа новой странице нашли текст и выдёргиваем его "{actual_prompt}"')

                        match = re.search(r'<!--\s*task_id:(.*?)\s*-->', actual_prompt)
                        matching_task = None
                        is_garbage = True

                        if match:
                            extracted_task_id = match.group(1)
                            if extracted_task_id and extracted_task_id != "None":
                                matching_task = next((task for task in PENDING_TASKS if task.get("task_id") == extracted_task_id), None)


                        if matching_task:
                            is_garbage = False 
                            log.info(f"ГЕНЕРАЦИЯ |\nНайдена задача для видео {video_id}: user_id {matching_task['user_id']}")    
                            video_tag = new_page.locator('video').first
                            direct_video_url = await video_tag.get_attribute('src')
                                



                            # 1. Формируем и отправляем РЕЗУЛЬТАТ для БОТА
                            result_for_bot = {
                                "event": "video_ready_for_user",
                                "user_id": matching_task['user_id'],
                                "video_url": direct_video_url,
                                "prompt": actual_prompt 
                            }
                            await redis_client.publish("results:to_bot", json.dumps(result_for_bot))
                            log.info(f"Результат для user_id {matching_task['user_id']} отправлен в 'results:to_bot'.")

                            # 2. Формируем и отправляем ПОДТВЕРЖДЕНИЕ для ДИСПЕТЧЕРА
                            completion_event = {
                                "event": "task_complete",
                                "worker_id": WORKER_ID,
                                "task_json": matching_task["task_json"]
                            }
                            await redis_client.publish(EVENTS_CHANNEL, json.dumps(completion_event))
                            log.info(f"Подтверждение о выполнении отправлено Диспетчеру в '{EVENTS_CHANNEL}'.")

                            PENDING_TASKS.remove(matching_task)
                            PROCESSED_VIDEO_IDS.add(video_id)
                            log.info(f"ГЕНЕРАЦИЯ |\nЗадача для видео {video_id} успешно обработана и удалена из очереди.")
                        else:
                            if not is_garbage:
                                log.error(f"ГЕНЕРАЦИЯ |\nНайдено готовое видео {video_id} с промптом '{actual_prompt}', но для него нет задачи в очереди.")
                            else:
                                log.warning(f"ГЕНЕРАЦИЯ | translate:Найдено 'мусорное' видео {video_id}. Оно будет проигнорировано и добавлено в обработанные.")
                            
                            PROCESSED_VIDEO_IDS.add(video_id)

                    except Exception as e:
                        log.error(f"ГЕНЕРАЦИЯ |\nОшибка при взаимодействии с новой страницей: {e}")

                except Exception as e:
                    log.error(f"ГЕНЕРАЦИЯ |\nОшибка при обработке видео {video_id or 'unknown'}: {e}")
                    if video_id:
                        log.warning(f"ГЕНЕРАЦИЯ |\nДобавляю проблемное видео {video_id} в 'обработанные', чтобы не повторять.")
                        PROCESSED_VIDEO_IDS.add(video_id)
                finally:
                    if new_page:
                        try:
                            await new_page.close()
                            log.info(f"ГЕНЕРАЦИЯ |\nНовая страница для видео ( {video_id or 'unknown'} ) закрыта.")
                        except Exception as e:
                            log.critical(f"ГЕНЕРАЦИЯ | Новая страница не закрылась: {e}")

            await asyncio.sleep(5)
            
        except Exception as e:
            log.error(f"ГЕНЕРАЦИЯ |\nКритическая ошибка в цикле мониторинга: {e}. Перезапуск через 10 секунд.")  
            await asyncio.sleep(10)
                      

async def submit_prompt_and_generate(page, prompt_text: str, task_id, redis_client, user_id):
    
    log.info(f"ГЕНЕРАЦИЯ | Начинаю отправку промта: на сайт...")
    prompt_with_id = f"{prompt_text} <!-- task_id:{task_id} -->"
    try:
        prompt_textarea = page.locator('textarea[placeholder="Describe your video..."]')
        await prompt_textarea.fill(prompt_with_id)
        log.info("ГЕНЕРАЦИЯ | Промт успешно вставлен в поле.")      

        send_button = page.locator('button:has-text("Create video")')
        await send_button.click(timeout=3000)
        log.info("ГЕНЕРАЦИЯ | Первая попытка отправки (клик по кнопке) 'Create video' нажата.")
        await page.wait_for_timeout(2000)

        current_text = await prompt_textarea.input_value()
        error_type = await check_for_errors_after_submit(page)
        if error_type:  # Если error (limit/token/general)
            success = await handle_error_after_submit(error_type, page, prompt_text, task_id, redis_client, user_id)
            return success  # False/True, early return без fallback

        if current_text == "":
            log.info("ГЕНЕРАЦИЯ | УСПЕХ! Отправка сработала с первого клика.")
            await check_and_close_cameo_popup(page)
            await save_debug_snapshot(page, prefix="success_submit_click")
        
        else:
            log.warning("ГЕНЕРАЦИЯ | Попытка 1 не удалась. Текст остался в поле. Шаг 4: Клик по координатам (897, 692).")
            await page.mouse.click(897, 692)
            await page.wait_for_timeout(2000)

            # --- ШАГ 5: Проверка успеха №2 ---
            current_text = await prompt_textarea.input_value()

            error_type = await check_for_errors_after_submit(page)
            if error_type:  # Если error (limit/token/general)
                success = await handle_error_after_submit(error_type, page, prompt_text, task_id, redis_client, user_id)
                return success  # False/True, early return без fallback


            if current_text == "":
                log.info("ГЕНЕРАЦИЯ | УСПЕХ! Отправка сработала после клика по координатам.")
                # await check_and_close_cameo_popup(page)
                await save_debug_snapshot(page, prefix="success_submit_coords")

                if success:
                    await save_debug_snapshot(page, prefix="success_submit_click")
                    log.info("ГЕНЕРАЦИЯ | Промт успешно отправлен, ошибок нет.")
                    return True
                return False  # Лимит или фатал (shutdown уже вызван)



            else:
                log.error("ГЕНЕРАЦИЯ | ВСЕ ПОПЫТКИ ПРОВАЛИЛИСЬ. Текст в поле. Перезапуск.")
                await save_debug_snapshot(page, prefix="FATAL_SUBMIT_ERROR")
                

                if error_type and error_type != 'daily_limit':
                    success = await handle_error_after_submit(error_type, page, prompt_text, task_id, redis_client, user_id)
                    if not success and error_type != 'daily_limit':  # Shutdown только non-limit
                        await graceful_shutdown("fatal during submit_prompt_and_generate", RUNTIME.r, RUNTIME.hb_task, RUNTIME.monitoring_task, RUNTIME.admin_listener_task, RUNTIME.context, RUNTIME.browser)
                        raise GracefulShutdown()
                else:
                    
                    await graceful_shutdown("fatal during submit_prompt_and_generate", RUNTIME.r, RUNTIME.hb_task, RUNTIME.monitoring_task, RUNTIME.admin_listener_task, RUNTIME.context, RUNTIME.browser)
                    raise GracefulShutdown()
                    # sys.exit(1)

    except Exception as e:
        log.error(f"ГЕНЕРАЦИЯ |\nОшибка при отправке промта: {e}")
        await save_debug_snapshot(page, prefix="error_prompt_submit_exception")
        log.error("ГЕНЕРАЦИЯ | Перезапуск воркера из-за непредвиденной ошибки в submit_prompt_and_generate.")
        await graceful_shutdown("fatal during submit_prompt_and_generate", RUNTIME.r, RUNTIME.hb_task, RUNTIME.monitoring_task, RUNTIME.admin_listener_task, RUNTIME.context, RUNTIME.browser)
        raise GracefulShutdown()
        # sys.exit(1)



async def handle_email_verification(page, redis_client):
    log.info("АУТЕНТИФИКАЦИЯ |\nОбнаружена страница верификации. Активирую ожидание кода из почтового сервиса.")
    try:
        personal_code_channel = f"codes:for:{WORKER_ID}"
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(personal_code_channel)

        log.info(f"АУТЕНТИФИКАЦИЯ |\nОжидаю код в Redis-канале: {personal_code_channel}. Таймаут: 2 минуты.")

        verification_code = None
        start_time = time.time()
        while time.time() - start_time < 120:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            try:
                if msg and msg.get("data"):
                    # Получаем данные. Они могут быть в байтах, декодируем в строку.
                    code_from_redis = msg.get("data")
                    if isinstance(code_from_redis, bytes):
                        code_from_redis = code_from_redis.decode('utf-8')

                    cleaned_code = code_from_redis.strip()
                    try:
                        if cleaned_code.isdigit():
                            verification_code = cleaned_code
                            log.info(f"АУТЕНТИФИКАЦИЯ |\nПолучен код верификации из Redis: {verification_code}")
                            break # Код получен, выходим из цикла ожидания
                        else:
                            log.warning(f"АУТЕНТИФИКАЦИЯ |\nПолучено нечисловое значение из канала Redis: {code_from_redis}")
                            
                    except Exception as e:
                        log.error(f"| АУТЕНТИФИКАЦИЯ \nНепредвиденная ошибка в ожидании код из тг 1: {e}")
                        await save_debug_snapshot(page, prefix="error_login")

            except Exception as e:
                log.error(f"| АУТЕНТИФИКАЦИЯ |\nНепредвиденная ошибка в ожидании код из тг 2: {e}")
                await save_debug_snapshot(page, prefix="error_login")


            
                
        await asyncio.sleep(0.1)

        if pubsub:
            await pubsub.unsubscribe(personal_code_channel)

            log.info("АУТЕНТИФИКАЦИЯ |\nОтписались от канала кодов")
        try:
            if not verification_code:
                log.error("АУТЕНТИФИКАЦИЯ |\nКод верификации не был получен за отведённое время.")
                return False

            try:
                log.info("АУТЕНТИФИКАЦИЯ |\nВвожу код верификации на странице...")
                await page.locator('input[name="code"]').fill(verification_code)
                current_url = page.url

                log.info(f"АУТЕНТИФИКАЦИЯ |\nНажимаю кнопку 'Continue'. Текущий URL: {current_url}")
                await page.get_by_role("button", name="Continue", exact=True).click()

                log.info("АУТЕНТИФИКАЦИЯ |\nОжидаю смены URL после ввода кода верификации...")
                await page.wait_for_url(lambda url: url != current_url, timeout=30000)
                log.info(f"АУТЕНТИФИКАЦИЯ |\nURL успешно изменился. Новый URL: {page.url}")
                await save_debug_snapshot(page, prefix="success_verification")


                return True
            
            except Exception as e:
                log.error(f"АУТЕНТИФИКАЦИЯ |\nНепредвиденная ошибка в поисках элементов на странице верификации: {e}")

            await page.wait_for_load_state("networkidle", timeout=30000)
            log.info("АУТЕНТИФИКАЦИЯ |\nКод верификации введён, продолжаю загрузку.")
            await save_debug_snapshot(page, prefix="success")
            return True


        except Exception as e:
            log.error(f"АУТЕНТИФИКАЦИЯ |\nОшибка на этапе верификации по email: {e}")
            await save_debug_snapshot(page, prefix="error_email_verification")
            return False
        
    except Exception as e:
        log.error(f"АУТЕНТИФИКАЦИЯ |\nНепредвиденная ошибка: {e}")




async def check_and_click_open_sora(page):
    log.info("АУТЕНТИФИКАЦИЯ |\nПроверяю наличие опциональной кнопки 'Open New Sora'...")
    try:
        sora_button = page.get_by_text("Open New Sora").first

        if await sora_button.is_visible(timeout=5000):
            log.info('АУТЕНТИФИКАЦИЯ |\nНайдена кнопка "Открыть новую сору", нажимаю')
            await sora_button.click()
            await page.wait_for_load_state('domcontentloaded', timeout=15000)
            log.info("АУТЕНТИФИКАЦИЯ |\nКнопка 'Open New Sora' нажата, страница загружена.")
        else:
            log.info("АУТЕНТИФИКАЦИЯ |\nКнопка 'Open New Sora' не найдена или не видна, продолжаю работу.")
            await save_debug_snapshot(page, prefix="success")
    
    except Exception:
        log.info("АУТЕНТИФИКАЦИЯ |\nКнопка 'Open New Sora' не найдена (это нормально), продолжаю работу.")


async def admin_commands_listener(page, redis: redis.Redis):
    log.info(f"Слушатель админских команд запущен и подписан на '{ADMIN_COMMANDS_CHANNEL}'.")

    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(ADMIN_COMMANDS_CHANNEL)

        while True:
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if not message:
                    continue

                data = json.loads(message["data"])
                target = data.get("target_worker")
                command = data.get("command")

                if target == WORKER_ID and command == "get_screenshot":
                    log.info(f"Получена персональная команда '{command}'. Выполняю.")
                    await make_and_send_screenshot(page, redis)
            except (json.JSONDecodeError, TypeError):
                pass
            except Exception as e:
                log.error(f"Ошибка в слушателе админ команд")


async def graceful_shutdown(reason, r, hb_task, monitoring_task, admin_listener_task, context, browser):
    log.error(f"GRACEFUL SHUTDOWN | {reason}")

    if PENDING_TASKS and r:
        log.warning(f"Возвращаю {len(PENDING_TASKS)} задач(и) в очередь '{RETURN_QUEUE}'")
        for task in list(PENDING_TASKS):
            try:
                await r.lpush(RETURN_QUEUE, task["task_json"])
            except Exception as e:
                log.error(f"Воркер {WORKER_ID} не вернул задачу: {e}")

    if r:
        try:
            await r.publish(EVENTS_CHANNEL, json.dumps({"worker_id": WORKER_ID, "event": "shutdown"}))
            log.info(f"{WORKER_ID} сообщил диспетчеру об отключении!")
        except Exception as e:
            log.error(f"{WORKER_ID} не смог уведомить диспетчера о шатдауне: {e}")

    for t in (hb_task, monitoring_task, admin_listener_task):
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    if context:
        try:
            log.info("Контекст закрыт")
            await context.close()
        except Exception as e:
            log.error(f"Ошибка в закрытии браузера: {e}")
    else:
        log.error("Контекста уже не существует на момент закрытимя")

    if browser and browser.is_connected():
        try:
            await browser.close()
            log.info("Браузер закрыт")
        except Exception as e:
            log.error(f"Браузер не был закрыт: {e}")
    else:
        log.error("Браузера не существует или он не был найден")

    if r:
        try:
            await r.aclose()
            log.info("Редис закрыт")
        except Exception as e:
            log.error(f"Ошибка в закрытии редиса {e}")
    else:
        log.error("Редис не существует или не был найден")



async def run_worker():
    # # Используем асинхронный клиент Redis
    r = None
    browser = None
    context = None
    hb_task = None
    monitoring_task = None
    admin_listener_task = None



    try:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            RUNTIME.r = r
            PERSONAL_TASK_CHANNEL = f"tasks:for:{WORKER_ID}"

            pw = await async_playwright().start()
            browser = await pw.chromium.launch(
                executable_path=pw.chromium.executable_path, # Указываем путь к браузеру Playwright
                headless=True,
                proxy={"server": PROXY_SERVER, "username": PROXY_USER, "password": PROXY_PASS},
            )
            RUNTIME.browser = browser
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                ignore_https_errors=True, 
                java_script_enabled=True,   
                viewport={"width": 1920, "height": 1080})
            RUNTIME.context = context
                
            page = await context.new_page()
            hb_task = asyncio.create_task(heartbeat(redis_client=r))
            RUNTIME.hb_task = hb_task
            monitoring_task = asyncio.create_task(video_monitoring_loop(page, r))
            RUNTIME.monitoring_task = monitoring_task
            admin_listener_task = asyncio.create_task(admin_commands_listener(page, r))
            RUNTIME.admin_listener_task = admin_listener_task


            
            log.info(f"АУТЕНТИФИКАЦИЯ |\nПерехожу на {TARGET_URL}")
            await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(3000)

            final_url = page.url
            log.info(f"АУТЕНТИФИКАЦИЯ |\nПосле перехода и паузы URL: {final_url}")

            if final_url.rstrip('/') != TARGET_URL.rstrip('/'):
                log.info("АУТЕНТИФИКАЦИЯ |\nURL изменился. Запускаю аутентификацию...")
                
                # Шаг 1: Выполняем основную попытку входа
                login_report = await perform_login(page)
                
                # Шаг 2: Проверяем, не требуется ли 2FA
                if "email-verification" in page.url:
                    log.info("АУТЕНТИФИКАЦИЯ |\nОбнаружена страница верификации, запускаю обработчик...")
                    verification_success = await handle_email_verification(page, r)
                    if not verification_success:
                        raise Exception("АУТЕНТИФИКАЦИЯ |\nЭтап верификации по email не был пройден.")
                
                # Шаг 3: Формируем ИТОГОВЫЙ отчёт и отправляем его ОДИН РАЗ
                final_login_status = "success" if TARGET_URL.split('/')[-1] in page.url else "failed"
                
                final_report = {
                    "worker": WORKER_ID,
                    "step": "authentication_flow",
                    "final_url": page.url,
                    "status": final_login_status
                }
                await r.publish(EVENTS_CHANNEL, json.dumps(final_report))
                log.info(f"АУТЕНТИФИКАЦИЯ |\nПроцесс аутентификации завершён со статусом: {final_login_status}")

                # Если мы не попали на целевую страницу, нет смысла продолжать
                if final_login_status == "failed":
                    raise Exception(f"АУТЕНТИФИКАЦИЯ |\nНе удалось достичь целевой страницы. Финальный URL: {page.url}")

                # Шаг 4: Только после УСПЕШНОЙ аутентификации ищем кнопку "Open New Sora"
                log.info("АУТЕНТИФИКАЦИЯ |\nИщу кнопку перехода на новую сору")
                sora_report = await check_and_click_open_sora(page)
                # await r.publish(EVENTS_CHANNEL, json.dumps({"worker": WORKER_ID, "step": "sora_button", **sora_report}))

                if TARGET_URL.rstrip('/') not in page.url.rstrip('/'):
                    log.warning(f"АУТЕНТИФИКАЦИЯ |\nПосле аутентификации мы на неправильной странице] ({page.url}).\nПринудительный переход на {TARGET_URL}")
                    await page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)
            else:
                log.info("АУТЕНТИФИКАЦИЯ |\nАутентификация не потребовалась, возможно мы уже на целевой странице.")

            try:
                log.info("АУТЕНТИФИКАЦИЯ |\nОжидаю появления поля для ввода промта...")
                prompt_textarea = page.locator('textarea[placeholder="Describe your video..."]')
                await prompt_textarea.wait_for(state="visible", timeout=20000)
                log.info("АУТЕНТИФИКАЦИЯ |\nИнтерфейс готов к работе!")
                await r.publish(EVENTS_CHANNEL, json.dumps({"event": "ready", "worker_id": WORKER_ID}))
                log.info(f"ЗАПУСК | Воркер {WORKER_ID} отправил сообщение о готовности в канал {EVENTS_CHANNEL}")

                await save_debug_snapshot(page, prefix="Succes_interface")
            except Exception as e:
                await save_debug_snapshot(page, prefix="error")
                raise Exception(f"АУТЕНТИФИКАЦИЯ |\nНе удалось дождаться загрузки интерфейса: {e}")
            
        except Exception as e:
            log.error(f"ЗАПУСК |\nКритическая ошибка во время начальной загрузки: {e}")
            await save_debug_snapshot(page, prefix="error_login")
            if monitoring_task:
                monitoring_task.cancel()
            log.info("Перезапускаю сервис!")
            await graceful_shutdown("fatal during submit_prompt_and_generate", RUNTIME.r, RUNTIME.hb_task, RUNTIME.monitoring_task, RUNTIME.admin_listener_task, RUNTIME.context, RUNTIME.browser)
            raise GracefulShutdown()
            # sys.exit(1)
    
        log.info(f"ЗАПУСК | Настройка завершена. Воркер {WORKER_ID} слушает свой персональный канал: {PERSONAL_TASK_CHANNEL}")
        await asyncio.sleep(2)

        async with r.pubsub() as pubsub:
            await pubsub.subscribe(PERSONAL_TASK_CHANNEL)
            async for msg in pubsub.listen():
                if msg is None or msg.get("type") != "message":
                    continue
                
                raw_data = msg.get("data")
                log.info(f"Получена команда: {raw_data}")

                try:
                    data = json.loads(raw_data)
                except json.JSONDecodeError:
                    log.error(f"Ошибка декодирования JSON: {raw_data}")
                    continue

                # Ваш код обработки команд get_ip, goto и т.д.
                cmd = data.get("cmd")
                payload = data.get("payload", {})

                if cmd == "stop":
                    log.info("Получена команда 'stop'. Завершаю работу.")
                    break
                elif cmd == "generate_video":
                    prompt = data.get("payload")
                    user_id = data.get("user_id")
                    task_json = data.get("task_json")
                    task_id = data.get("task_id")


                    if not prompt or not user_id or not task_json:
                        log.warning(f"ГЕНЕРАЦИЯ |\nНеполные данные для generate_video: {data}")
                        continue

                    submit_ok = await submit_prompt_and_generate(page, prompt, task_id, r, user_id)
                    
                    if submit_ok:
                        PENDING_TASKS.append({
                            "task_id": task_id,
                            "user_id": user_id, 
                            "prompt": prompt,
                            "start_time": time.time(),
                            "task_json": task_json,
                            "timed_out": False
                            })
                        log.info(f"Задача для user_id ( {user_id} ) принята в работу.")
                    else:
                        log.info("ГЕНЕРАЦИЯ | Задача не отправлена (лимит/ошибка). Диспетчер перераспределит.")
                        continue


                elif cmd and cmd.startswith("screen_"):
                    # worker_num_str = cmd.split('_')[-1]
                    await make_and_send_screenshot(page, r)

                else:
                    await r.publish(EVENTS_CHANNEL, json.dumps({"worker": WORKER_ID, "error": f"unknown cmd: {cmd}"}))
    except asyncio.CancelledError:
        log.warning("Главная задача воркера была отменена. Начинаю грациозное завершение...")
    except GracefulShutdown:
        log.warning("GracefulShutdown: воркер завершился по запросу")
    except Exception as e:
        log.critical(f"Критическая ошибка в run_worker: {e}", exc_info=True)
    finally:

        log.info("ЗАПУСК |\nОстанавливаем воркера...")
        # global RUNTIME

        if r:
            if PENDING_TASKS:
                log.warning(f"Обнаружено {len(PENDING_TASKS)} незавершенных задач. Возвращаю их в очередь '{RETURN_QUEUE}'.")
                for task in PENDING_TASKS:
                    try:
                        await r.lpush(RETURN_QUEUE, task["task_json"])
                    except asyncio.CancelledError:
                        log.error("Не удалось вернуть задачу в очередь из-за отмены операции.")
                        break
                log.info("Все незавершенные задачи успешно возвращены.")
            try:
                await r.publish(EVENTS_CHANNEL, json.dumps({"worker_id": WORKER_ID, "event": "shutdown"}))
                log.info(f"Воркер {WORKER_ID} сообщил диспетчеру о завершении работы!")
            except asyncio.CancelledError:
                log.error("Не удалось отправить сообщение о выключении Диспетчеру.")
        
        if hb_task:
            hb_task.cancel()

        if monitoring_task:
            log.info("ЗАПУСК |\nОстанавливаем мониторинг новых видео")
            monitoring_task.cancel()
        if 'admin_listener_task' in locals() and admin_listener_task:
            admin_listener_task.cancel()

        if context:
            await context.close()
        if browser and browser.is_connected():
            await browser.close()
        if r:
            await r.aclose()




async def make_and_send_screenshot(page, redis_client):
    log.info("СКРИНШОТЫ |\nПолучена команда на создание скриншота...")
    try:
        quality = 30
        screenshot_bytes = await page.screenshot(type='jpeg', quality=quality)
        quality = 30
        page_title = await page.title()
        page_url = page.url

        # 1. Получаем путь к директории проекта (на уровень выше текущего файла)
        script_dir  = Path(__file__).resolve().parent


        timestamp = int(time.time())
        screenshot_filename = f"temp_screenshot_{WORKER_ID}_{timestamp}.jpeg"

        screenshot_path = script_dir / screenshot_filename
        screenshot_path.write_bytes(screenshot_bytes)

        log.info(f"СКРИНШОТЫ | Скриншот сохранен в: {screenshot_path}")

        caption_text = (
            f"ℹ️ Скриншот от воркера **{WORKER_ID}**\n\n"
            f"**Title:** `{page_title}`\n"
            f"**URL:** `{page_url}`"
        )
        
        message_to_bot = {
            "event": "screenshot_ready",
            "message": caption_text,
            "screenshot_path": str(screenshot_path)
        }

        await redis_client.publish("results:to_bot", json.dumps(message_to_bot))
        log.info("СКРИНШОТЫ |\nЗапрос на отправку скриншота отправлен в Redis.")
        
    except Exception as e:
        log.error(f"СКРИНШОТЫ |\nОшибка при создании скриншота: {e}")

async def heartbeat(redis_client):
    key = f"hb:{WORKER_ID}"
    limit_key = f"worker_limits:{WORKER_ID}"

    while True:
        try:
            exhausted_raw = await redis_client.hget(limit_key, "is_exhausted")
            is_exhausted = exhausted_raw == "1"
            current_pending_count = len(PENDING_TASKS) if not is_exhausted else 999

            log.info(f"HEARTBEAT | {WORKER_ID}: pending_tasks={current_pending_count}, exhausted={is_exhausted}")

            await redis_client.set(key, "1", ex=12)

            heartbeat_event = {
                "worker_id": WORKER_ID, 
                "event": "heartbeat", 
                "ts": time.time(), 
                "pending_tasks": current_pending_count}

            await redis_client.publish(EVENTS_CHANNEL, json.dumps(heartbeat_event))

            log.info(f"HEARTBEAT | Отправлен heartbeat для {WORKER_ID} (pending: {current_pending_count})")


        except Exception as e:
            log.error(f"HEARTBEAT | Ошибка при отправке heartbeat для {WORKER_ID}: {e}")
        await asyncio.sleep(5)



async def main():
    main_task = asyncio.create_task(run_worker())
    def signal_handler():
        if RUNTIME.shutting_down:
            return
        RUNTIME.shutting_down = True
        log.warning("Получен сигнал на завершение! Отменяю главную задачу...")
        asyncio.create_task(
            graceful_shutdown(
                "signal",
                RUNTIME.r,
                RUNTIME.hb_task,
                RUNTIME.monitoring_task,
                RUNTIME.admin_listener_task,
                RUNTIME.context,
                RUNTIME.browser,
            )   

        )
        if not main_task.done():
            main_task.cancel()
        # if not main_task.done():
        #     main_task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await main_task
    except asyncio.CancelledError:
        log.info("Главная задача была отменена, как и ожидалось при завершении.")

if __name__ == "__main__":
    log.info("\nСкрипт запущен")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Программа прервана вручную (KeyboardInterrupt)")
    finally:
        log.info("Воркер полностью остановлен.")
    