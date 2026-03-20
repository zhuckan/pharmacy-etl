import warnings
warnings.filterwarnings("ignore", message="pkg_resources is deprecated")

import mysql.connector
import re
from config import DB_CONFIG
from pymorphy2 import MorphAnalyzer

class SearchEngine:
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None
        self.morph = MorphAnalyzer()
        self.stop_words = {
            'привет', 'здравствуйте', 'здравствуй', 'ку', 'добрый', 'день', 'вечер', 'утро',
            'показать', 'показывать', 'найти', 'искать', 'находить', 'посмотреть', 'смотреть',
            'увидеть', 'видеть', 'выдать', 'выводить', 'отобразить', 'отображать',
            'я', 'мы', 'ты', 'вы', 'он', 'она', 'оно', 'они', 'мой', 'твой', 'наш', 'ваш', 'свой',
            'этот', 'тот', 'такой', 'который', 'какой', 'что', 'кто', 'как', 'так', 'там', 'тут',
            'здесь', 'туда', 'сюда', 'куда', 'где', 'откуда', 'почему', 'зачем', 'весь', 'сам',
            'и', 'в', 'во', 'с', 'со', 'у', 'к', 'ко', 'за', 'над', 'под', 'о', 'об', 'от', 'ото',
            'по', 'при', 'про', 'без', 'до', 'для', 'из', 'изо', 'между', 'через', 'сквозь',
            'вокруг', 'около', 'возле', 'рядом', 'перед', 'после', 'внутри', 'снаружи', 'на',
            'или', 'либо', 'то', 'чтобы', 'чтоб', 'если', 'хотя', 'пусть', 'давай', 'давайте',
            'будто', 'словно', 'не', 'ни', 'бы', 'б', 'ну', 'вот', 'вон',
            'аптека', 'аптечный', 'улица', 'проспект', 'переулок', 'бульвар', 'площадь',
            'аллея', 'шоссе', 'набережная', 'город', 'деревня', 'посёлок',
            'находиться', 'расположить', 'быть', 'стать', 'являться', 'считаться', 'называться',
            'иметь', 'мочь', 'нужный', 'нужно', 'надо', 'можно', 'нельзя',
            'всякий', 'каждый', 'любой', 'другой', 'разный', 'различный',
            'всё', 'пожалуйста'
        }

    def _connect(self):
        self.conn = mysql.connector.connect(**self.config)
        self.cursor = self.conn.cursor(dictionary=True)

    def _get_search_groups(self, query):
        clean = re.sub(r'[^a-zA-Zа-яА-ЯёЁ\s]', ' ', query.lower())
        words = clean.split()
        numbers = re.findall(r'\d+', query)

        groups = []
        for word in words:
            normal = self.morph.parse(word)[0].normal_form
            if normal in self.stop_words:
                continue
            groups.append(normal)

        for num in numbers:
            groups.append(num)

        return groups

    def run(self):
        self._connect()
        print("Привет! Я помогу тебе найти аптеку. Укажи город, улицу или номер аптеки (например: 'аптеки в Минске' или 'улица Ленина 5').")

        try:
            while True:
                text = input("Поиск: ").strip()
                if not text:
                    continue

                groups = self._get_search_groups(text)
                if not groups:
                    print("Запрос не содержит значимых слов. Пожалуйста, уточните город, улицу или номер аптеки.\n")
                    continue

                conditions = []
                params = []
                sql_condition = "(LOWER(pharmacyName) LIKE %s OR LOWER(pharmacyNumber) LIKE %s OR LOWER(locality) LIKE %s OR LOWER(street) LIKE %s OR LOWER(houseNumber) LIKE %s OR LOWER(pharmacyPhoneNumber) LIKE %s)"

                for group in groups:
                    conditions.append(sql_condition)
                    params.extend([f'%{group}%'] * 6)

                sql = f"""
                    SELECT pharmacyName, pharmacyNumber, locality, street, houseNumber, pharmacyPhoneNumber
                    FROM pharmacies
                    WHERE {' OR '.join(conditions)}
                """

                self.cursor.execute(sql, params)
                rows = self.cursor.fetchall()

                if not rows:
                    print("По вашему запросу ничего не найдено. Попробуйте изменить формулировку или указать другие данные.\n")
                elif len(rows) > 10:
                    print("Найдено слишком много вариантов (больше 10). Пожалуйста, уточните улицу или добавьте более конкретные параметры поиска.\n")
                else:
                    for row in rows:
                        print(
                            f"Название: {row['pharmacyName'] or '—'} "
                            f"Номер: {row['pharmacyNumber'] or '—'} "
                            f"Населённый пункт: {row['locality'] or '—'} "
                            f"Улица: {row['street'] or '—'} "
                            f"Дом: {row['houseNumber'] or '—'} "
                            f"Телефон: {row['pharmacyPhoneNumber'] or '—'} "
                        )
                    print()
        except KeyboardInterrupt:
            print("\nПоиск завершён.")
