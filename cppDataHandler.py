import json
import pandas as pd
import sqlite3
import os
import sys
from loguru import logger

class cppDataHandler:
    def __init__(self, csvPath="default.csv", dbPath="default.db", force=False):
        if os.path.exists(csvPath):
            if force:
                os.remove(csvPath)
            else:
                logger.error(f"CSV file {csvPath} already exists")
                exit(1)
        self.csvPath = csvPath
        self.dbPath = dbPath
        self.csvFirstWrite = True
        return

    def writeCSV(self, data):
        is_iterator = hasattr(data, '__iter__') and not isinstance(data, (str, dict))

        if is_iterator:
            for item in data:
                self._writeCSVrow(item)
        else:
            self._writeCSVrow(data)
        return
    
    def _writeCSVrow(self, data: dict):
        df = pd.DataFrame([data])
        mode = 'w' if self.csvFirstWrite else 'a'

        df.to_csv(self.csvPath, index=False,
                  mode=mode, encoding='utf-8',
                  header=self.csvFirstWrite)
        self.csvFirstWrite = False
        return
    
    def writeDB(self, data):
        # 后面再说
        raise NotImplementedError
        return