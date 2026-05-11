# Pharmacy ETL & Search Service

A pet project for automatic loading of pharmacy data from Excel into MySQL with lemmatization-based search.

## Features

- 📁 Monitors a folder for Excel files (`.xls`, `.xlsx`).
- ⚙️ Parses files with configurable skip rows (`config.py`).
- 🧹 Data cleaning: converts pharmacy numbers to integers, handles empty values.
- 🗄️ Creates and populates tables `pharmacies`, `file_list`, `status` in MySQL.
- 🔍 Duplicate check before insert (uniqueness across all fields).
- 🚀 Batch insert and multi‑threaded file processing (up to 5 workers).
- 📝 Logs file processing status (success/error) into the `file_list` table.
- 📦 Processed files are moved to an archive folder and renamed by file ID.
- 💬 Lemmatization‑based search engine with pymorphy2 and stop‑word filtering.
- ❌ Fault‑tolerant: errors in one file do not stop the service.

## Technologies

- **Python 3.8+**
- **Pandas** – Excel parsing, data manipulation
- **mysql-connector-python** – MySQL interface
- **pymorphy2** – lemmatization for Russian language
- **concurrent.futures / threading** – multithreading
- **MySQL** – database

## Installation & Setup

### 1. Clone the repository

```bash
git clone https://github.com/zhuckan/pharmacy-etl.git
cd pharmacy-etl

### 2. Install dependencies
It is recommended to use a virtual environment:

bash
python -m venv venv
source venv/bin/activate    # Linux / macOS
venv\Scripts\activate       # Windows

Install required packages:

bash
pip install pandas mysql-connector-python pymorphy2 openpyxl

### 3. Configure MySQL
Make sure MySQL is running. Create a database (e.g., pharmacies_db) and update config.py with your connection details:

python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',   # leave empty if no password
    'database': 'pharmacies_db',
}
Other settings in config.py can be left as default.

### 4. Create necessary folders
By default, the service expects the following directories:

uploaded_files – place Excel files here for processing.

processed – processed files are moved here (subfolders by date are created automatically).

You can change these paths in config.py.

### 5. Usage
Start the ETL watcher
bash
python main.py
The service will scan uploaded_files every 5 seconds (configurable).
Place an Excel file inside – it will be processed automatically. Logs appear in the console.

### 6. Launch the search module
bash
python search_main.py
An interactive search prompt opens. Example queries:

аптеки на Ленина (pharmacies on Lenin street)

Минск, улица Янки Купалы 15

номер 123 (number 123)

+375 29 1234567

The engine normalizes words, removes stop‑words, and returns matches.
If more than 10 results are found, it asks to refine the query.

uploaded_files/ – папка для входящих файлов (нужно создать).

processed/ – папка для архива (создаётся автоматически).

