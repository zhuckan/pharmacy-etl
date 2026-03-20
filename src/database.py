import mysql.connector
import pandas as pd
from datetime import datetime
import threading

class Database:
    _insert_lock = threading.Lock()

    def __init__(self, config):
        self._config = config
        self._conn = None
        self._cursor = None
        self._connect()

    def _connect(self):
        self._conn = mysql.connector.connect(
            host=self._config['host'],
            user=self._config['user'],
            password=self._config['password']
        )
        self._cursor = self._conn.cursor()
        self._cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self._config['database']}")
        self._cursor.execute(f"USE {self._config['database']}")

    def create_status_table(self):
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS status (
                id INT PRIMARY KEY,
                result VARCHAR(50) NOT NULL
            )
        """)
        self._cursor.execute("INSERT IGNORE INTO status (id, result) VALUES (1, 'Успешно'), (2, 'Ошибка')")
        self._conn.commit()

    def create_fileList_table(self):
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS file_list (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                date DATETIME NOT NULL,
                statusId INT NOT NULL,
                FOREIGN KEY (statusId) REFERENCES status(id)
            )
        """)
        self._conn.commit()

    def insert_fileList(self, file_name, status_id):
        received_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self._cursor.execute(
            "INSERT INTO file_list (name, date, statusId) VALUES (%s, %s, %s)",
            (file_name, received_date, status_id)
        )
        self._conn.commit()
        return self._cursor.lastrowid

    def create_and_fill_pharmacies(self, df):
        self._cursor.execute("""
            CREATE TABLE IF NOT EXISTS pharmacies (
                id INT AUTO_INCREMENT PRIMARY KEY,
                pharmacyName VARCHAR(255),
                pharmacyNumber INT,
                locality VARCHAR(100),
                street VARCHAR(255),
                houseNumber VARCHAR(50),
                pharmacyPhoneNumber VARCHAR(50)
            )
        """)
        self._batch_insert(df)

    def _batch_insert(self, df):
        with Database._insert_lock:
            batch_size = 50
            batch = []
            insert_sql = """
                INSERT IGNORE INTO pharmacies 
                (pharmacyName, pharmacyNumber, locality, street, houseNumber, pharmacyPhoneNumber) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            unique_sql = """
                SELECT 'duplicate' FROM pharmacies
                WHERE pharmacyName <=> %s
                  AND pharmacyNumber <=> %s
                  AND locality <=> %s
                  AND street <=> %s
                  AND houseNumber <=> %s
                  AND pharmacyPhoneNumber <=> %s
            """
            for _, row in df.iterrows():
                record = tuple(None if pd.isna(v) else v for v in row)
                self._cursor.execute(unique_sql, record)
                result = self._cursor.fetchone()
                if result is None:
                    batch.append(record)
                    if len(batch) == batch_size:
                        self._cursor.executemany(insert_sql, batch)
                        batch = []
            if batch:
                self._cursor.executemany(insert_sql, batch)
            self._conn.commit()
