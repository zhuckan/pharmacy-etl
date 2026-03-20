from config import DB_CONFIG
from search_engine import SearchEngine

engine = SearchEngine(DB_CONFIG)
engine.run()