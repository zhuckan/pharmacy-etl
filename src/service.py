import os
import time
import shutil
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from config import *
from excel_parser import ExcelParser
from database import Database

class DataWatcher:
    def __init__(self):
        self._watch_directory = UPLOAD_DIR
        self._processed_dir = PROCESSED_DIR
        self._check_interval = CHECK_INTERVAL
        self._executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self._files_in_progress = set()
        self._lock = threading.Lock()

    def run(self):
        print("\nСервис запущен")
        db_init = Database(DB_CONFIG)
        db_init.create_status_table()
        db_init.create_fileList_table()

        try:
            while True:
                items = os.listdir(self._watch_directory)
                files = [file for file in items if file.lower().endswith(ALLOWED_EXTENSIONS)]

                for filename in files:
                    self._executor.submit(self._process_file, filename)

                time.sleep(self._check_interval)

        except KeyboardInterrupt:
            print("\nСервис остановлен")
            self._executor.shutdown(wait=True)
        except Exception as e:
            print(f"[{datetime.now()}] Ошибка в основном цикле: {e}")
            self._executor.shutdown(wait=False)

    def _process_file(self, filename):
        thread_id = threading.get_ident()

        with self._lock:
            if filename in self._files_in_progress:
                return
            self._files_in_progress.add(filename)

        print(f"[{datetime.now()}] Поток {thread_id} начал обработку: {filename}")
        filepath = os.path.join(self._watch_directory, filename)
        base, ext = os.path.splitext(filename)
        
        parser = ExcelParser(skiprows=SKIP, columns=COLUMNS)
        db = Database(DB_CONFIG)
        
        try:
            df = parser.parse(filepath)
            df = parser.parse_data(df)
            db.create_and_fill_pharmacies(df)
            
            file_id = db.insert_fileList(filename, 1)
            print(f"[{datetime.now()}] Поток: {thread_id}. Успешно: {filename} -> ID {file_id}")

        except Exception as error:
            file_id = db.insert_fileList(filename, 2)
            print(f"[{datetime.now()}] Поток: {thread_id}. Ошибка: {filename} -> ID {file_id} ({error})")

        date_str = datetime.now().strftime('%Y-%m-%d')
        dest_dir = os.path.join(self._processed_dir, date_str)
        os.makedirs(dest_dir, exist_ok=True)

        new_filename = f"{file_id}{ext}"
        destination = os.path.join(dest_dir, new_filename)
        shutil.move(filepath, destination)

        with self._lock:
            self._files_in_progress.discard(filename)
