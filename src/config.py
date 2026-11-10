import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'pharmacies_db'),
}

SKIP_ROWS = int(os.getenv('SKIP_ROWS', 2))
COLUMNS = ['pharmacyName', 'pharmacyNumber', 'locality', 'street', 'houseNumber', 'pharmacyPhoneNumber']

# Используем относительные пути, чтобы проект работал в Docker и на любом ПК
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
UPLOAD_DIR = os.path.join(BASE_DIR, os.getenv('UPLOAD_DIR', 'repository/uploaded_files'))
PROCESSED_DIR = os.path.join(BASE_DIR, os.getenv('PROCESSED_DIR', 'repository/processed'))

ALLOWED_EXTENSIONS = ('.xls', '.xlsx')
CHECK_INTERVAL = int(os.getenv('CHECK_INTERVAL', 5))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', 5))