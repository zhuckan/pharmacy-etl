import logging
import sys

logging.basicConfig(level=logging.WARNING, stream=sys.stdout)

from config import DB_CONFIG
from search_engine import SearchEngine

if __name__ == "__main__":
    engine = SearchEngine(DB_CONFIG)
    engine.run()