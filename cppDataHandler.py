import json
import pandas as pd
import sqlite3
import os
import sys
from loguru import logger
from tqdm import tqdm
import threading
import itertools

class cppDataHandler:
    def __init__(self, path="default", db_id='2231', force=False, commit_every=1000):
        csvPath = path + ".csv"
        dbPath = db_id + ".db"
        if os.path.exists(csvPath):
            if force:
                os.remove(csvPath)
                #os.remove(dbPath)
                logger.warning(f"CSV file {csvPath} already exists, overwriting")
            else:
                logger.error(f"CSV file {csvPath} already exists")
                exit(1)

        self.csvPath = csvPath
        self.dbPath = dbPath
        self.csvFirstWrite = True
        self.lock = threading.Lock()
        self.commit_every = commit_every
        return
    
    def writeAll(self, data):
        if data is None:
            logger.warning("No data to write!")
            return
        # 单条 dict → list 包一层
        if isinstance(data, dict):
            data_list = [data]
        # 其他 iterable（包括 list、生成器等）
        elif hasattr(data, '__iter__') and not isinstance(data, (str,)):
            data_list = list(data)
        else:
            logger.warning(f"Unsupported data type: {type(data)}")
            return

        if not data_list:
            logger.warning("No data to write after conversion!")
            return
        
        self.writeDB(data_list)
        self.writeCSV(data_list)
    
    def writeCSV(self, data: list):
        df = pd.DataFrame(data)
        mode = 'w' if self.csvFirstWrite else 'a'

        df.to_csv(self.csvPath, index=False,
                mode=mode, encoding='utf-8',
                header=self.csvFirstWrite)
        if self.csvFirstWrite:
            with self.lock:
                logger.info(f"CSV file {self.csvPath} created")
                self.csvFirstWrite = False
        return
    
    def _convert_sql_value(self, val):
        if isinstance(val, (list, dict)):
            return json.dumps(val, ensure_ascii=False)
        elif isinstance(val, bool):
            return int(val)
        return val
    
    def writeDB(self, data: list):
        if not data:
            logger.warning("No data to write to DB.")
            return
        
        table_name = os.path.splitext(os.path.basename(self.csvPath))[0]
        first_row = data[0]
        columns = list(first_row.keys())

        # infererence of data types
        def infer_type(value):
            if isinstance(value, int):
                return "INTEGER"
            elif isinstance(value, float):
                return "REAL"
            elif isinstance(value, bool):
                return "INTEGER"
            elif value is None:
                return "TEXT"
            else:
                return "TEXT"

        # ➡️ 逐列分析，采样前 N 条
        def infer_column_type(col_name, sample_data, sample_size=10):
            # 限制取样数据数量
            samples = [row.get(col_name) for row in sample_data[:sample_size] if col_name in row]
            types = set(infer_type(val) for val in samples if val is not None)
            # 优先级 INTEGER < REAL < TEXT
            if not types:
                return "TEXT"
            if "TEXT" in types:
                return "TEXT"
            if "REAL" in types:
                return "REAL"
            return "INTEGER"

        # 获取最终表结构定义
        col_defs = []
        for col in columns:
            col_type = infer_column_type(col, data)
            col_defs.append(f'"{col}" {col_type}')
        
        create_table_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)});'

        with self.lock:
            conn = sqlite3.connect(self.dbPath)
            cursor = conn.cursor()

            try:
                logger.debug(f"Creating table '{table_name}' with SQL: {create_table_sql}")
                cursor.execute(create_table_sql)

                placeholders = ', '.join(['?'] * len(columns))
                column_names_quoted = ', '.join([f'"{col}"' for col in columns])
                insert_sql = f'INSERT INTO "{table_name}" ({column_names_quoted}) VALUES ({placeholders})'


                # 准备批量插入数据
                rows_to_insert = []
                for row in data:
                    row_values = [self._convert_sql_value(row.get(col, None)) for col in columns]
                    rows_to_insert.append(row_values)

                for i in range(0, len(rows_to_insert), self.commit_every):
                    batch = rows_to_insert[i:i+self.commit_every]
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    logger.debug(f"Committed batch of {len(batch)} rows")

                logger.debug(f"Inserted {len(rows_to_insert)} rows into DB table '{table_name}'")
            except Exception as e:
                logger.error(f"DB write failed: {e}")
            finally:
                conn.close()
