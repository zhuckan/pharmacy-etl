DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'pharmacies_db',
}

SKIP = 2
COLUMNS = ['pharmacyName', 'pharmacyNumber', 'locality', 'street', 'houseNumber', 'pharmacyPhoneNumber']

UPLOAD_DIR = '/Users/sophie/Documents/Pharmacy/repository/uploaded_files'
PROCESSED_DIR = '/Users/sophie/Documents/Pharmacy/repository/processed'

ALLOWED_EXTENSIONS = ('.xls', '.xlsx')
CHECK_INTERVAL = 5
MAX_WORKERS = 5
