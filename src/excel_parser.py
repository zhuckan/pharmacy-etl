import logging
import pandas as pd
from typing import List, Optional

logger = logging.getLogger(__name__)

class ExcelParser:
    def __init__(self, skiprows: Optional[int] = None, columns: Optional[List[str]] = None):
        self._skiprows = skiprows
        self._columns = columns

    def parse(self, path: str) -> pd.DataFrame:
        logger.debug(f"Parsing file: {path}")
        df = pd.read_excel(path, skiprows=self._skiprows, header=None, usecols=range(1, 7))
        return df

    def parse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.shape[0] == 0:
            raise ValueError("Файл не содержит данных")
        df.columns = self._columns
        df['pharmacyNumber'] = df['pharmacyNumber'].apply(self._clean_pharmacy_number)
        return df

    def _clean_pharmacy_number(self, value: any) -> Optional[int]:
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        return None
