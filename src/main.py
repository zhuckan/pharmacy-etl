import logging
import sys
import os

# Настройка логирования в консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)

from service import DataWatcher

if __name__ == "__main__":
    watcher = DataWatcher()
    watcher.run()