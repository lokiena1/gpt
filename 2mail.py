import os
import time
import re
import redis
import logging
import email
from email import policy

# --- Конфигурация ---

MAIL_CONFIG = {
    "worker-1": {
        "new": "/home/sora_worker1/Maildir/new",
        "cur": "/home/sora_worker1/Maildir/cur"
    },
    "worker-2": {
        "new": "/home/sora_worker2/Maildir/new",
        "cur": "/home/sora_worker2/Maildir/cur"
    },
}
# Убедитесь, что пути соответствуют вашему пользователю

REDIS_HOST = "localhost"
REDIS_PORT = 6379

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/mail.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("local-mail-parser")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def find_code_in_text(text):
    """Ищет 6-значный код в тексте."""
    if not text:
        return None
    match = re.search(r'\b\d{6}\b', text)
    if match:
        return match.group(0)
    return None

def parse_email_file(filepath):
    """Парсит email файл используя стандартную библиотеку Python."""
    try:
        with open(filepath, 'rb') as f:
            msg = email.message_from_binary_file(f, policy=policy.default)
        
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        charset = part.get_content_charset() or 'utf-8'
                        body = payload.decode(charset, errors='replace')
                        break
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                charset = msg.get_content_charset() or 'utf-8'
                body = payload.decode(charset, errors='replace')
        
        return body
    except Exception as e:
        log.error(f"Ошибка парсинга файла {filepath}: {e}")
        return None

def main_loop():
    """Основной цикл мониторинга директории."""
    log.info("=" * 60)
    log.info("Запущен мульти-воркерный почтовый парсер")
    log.info("=" * 60)

    while True:
        try:

            for worker_id, paths in MAIL_CONFIG.items():
                mail_dir = paths["new"]
                processed_dir = paths["cur"]

                
                if not os.path.exists(mail_dir):
                    log.warning(f"Директория {mail_dir} для воркера {worker_id} не найдена. Пропускаю.")                
                    time.sleep(2)
                    continue
                    
                new_letters = [f for f in os.listdir(mail_dir) 
                            if os.path.isfile(os.path.join(mail_dir, f))]
                    
                if not new_letters:
                    log.info(f"Для воркера {worker_id} нет новых писем. Пропускаю.")
                    continue

                log.info(f"Обнаружено {len(new_letters)} писем для воркера {worker_id}")
            
                for filename in new_letters:
                    filepath = os.path.join(mail_dir, filename)
                    log.info(f"🔔 Найдено новое письмо: {filename}")
                        
                    body = parse_email_file(filepath)
                    if body:
                        code = find_code_in_text(body)
                        
                        if code:
                            personal_channel = f"codes:for:{worker_id}"
                            log.info(f"✅ Найден код: {code} для {worker_id}")
                            r.publish(personal_channel, code)
                            log.info(f"📤 Код {code} отправлен в Redis канал '{personal_channel}'")
                        else:
                            log.warning("⚠️ Код не найден в теле письма")
                    
                    try:
                        os.rename(filepath, os.path.join(processed_dir, filename))
                        log.info(f"✓ Письмо {filename} для {worker_id} перемещено.")
                    except Exception as e:
                            log.error(f"Ошибка при перемещении файла для {worker_id}: {e}")

            time.sleep(5)

        except KeyboardInterrupt:
            log.info("\n⛔ Программа остановлена пользователем")
            break
        except Exception as e:
            log.error(f"❌ Непредвиденная ошибка: {e}", exc_info=True)
            time.sleep(30)

if __name__ == "__main__":

    main_loop()

