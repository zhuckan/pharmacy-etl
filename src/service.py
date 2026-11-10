import logging
import os
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from config import UPLOAD_DIR, PROCESSED_DIR, CHECK_INTERVAL, MAX_WORKERS, ALLOWED_EXTENSIONS, SKIP_ROWS, COLUMNS, \
    DB_CONFIG
from excel_parser import ExcelParser
from database import Database

logger = logging.getLogger(__name__)


class DataWatcher:
    def __init__(self):
        self._watch_directory = UPLOAD_DIR
        self._processed_dir = PROCESSED_DIR
        self._check_interval = CHECK_INTERVAL
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._files_in_progress = set()
        self._lock = threading.Lock()

        os.makedirs(self._watch_directory, exist_ok=True)
        os.makedirs(self._processed_dir, exist_ok=True)

    def run(self) -> None:
        logger.info("Сервис запущен. Ожидание файлов...")
        db_init = Database(DB_CONFIG)
        db_init.create_status_table()
        db_init.create_file_list_table()

        try:
            while True:
                items = os.listdir(self._watch_directory)
                files = [file for file in items if file.lower().endswith(ALLOWED_EXTENSIONS)]

                for filename in files:
                    self._executor.submit(self._process_file, filename)

                time.sleep(self._check_interval)

        except KeyboardInterrupt:
            logger.info("Сервис остановлен пользователем")
            self._executor.shutdown(wait=True)
        except Exception as e:
            logger.critical(f"Ошибка в основном цикле: {e}", exc_info=True)
            self._executor.shutdown(wait=False)

    def _process_file(self, filename: str) -> None:
        thread_id = threading.get_ident()

        with self._lock:
            if filename in self._files_in_progress:
                return
            self._files_in_progress.add(filename)

        logger.info(f"[Thread {thread_id}] Начал обработку: {filename}")
        filepath = os.path.join(self._watch_directory, filename)
        _, ext = os.path.splitext(filename)

        parser = ExcelParser(skiprows=SKIP_ROWS, columns=COLUMNS)
        db = Database(DB_CONFIG)

        file_id = None
        try:
            df = parser.parse(filepath)
            df = parser.parse_data(df)
            db.create_and_fill_pharmacies(df)

            file_id = db.insert_file_list(filename, 1)
            logger.info(f"[Thread {thread_id}] Успешно: {filename} -> ID {file_id}")

        except Exception as error:
            file_id = db.insert_file_list(filename, 2)
            logger.error(f"[Thread {thread_id}] Ошибка: {filename} -> ID {file_id} ({error})")

        finally:
            if file_id:
                date_str = datetime.now().strftime('%Y-%m-%d')
                dest_dir = os.path.join(self._processed_dir, date_str)
                os.makedirs(dest_dir, exist_ok=True)

                new_filename = f"{file_id}{ext}"
                destination = os.path.join(dest_dir, new_filename)
                try:
                    shutil.move(filepath, destination)
                    logger.debug(f"Файл перемещен в архив: {destination}")
                except Exception as e:
                    logger.error(f"Не удалось переместить файл {filename}: {e}")

            with self._lock:
                self._files_in_progress.discard(filename)
