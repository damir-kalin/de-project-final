import os
from datetime import datetime, timedelta
from logging import Logger

import pandas as pd
from vertica_python import Connection
import contextlib

from lib.pg_connect import PgConnect
from lib.vertica_connect import VerticaConnect

PATH_SQL = '/lessons/dags/sql'

class OriginRepository:
    def __init__(self, pg: PgConnect) -> pd.DataFrame:
        self._db = pg
    def get_df(self, seach_date: datetime, table:str) -> pd.DataFrame:
        with self._db.client() as engine:
            with open(PATH_SQL + f'/pg.select_{table}.sql', 'r') as script:
                sql_script = script.read().format(seach_date)
                print(sql_script)
                df = pd.read_sql(
                    sql=sql_script,
                    con=engine
                )
        return df
    
class DestRepository:
    def df_insert(self, conn: Connection, df: pd.DataFrame, table:str, logger: Logger) -> None:
        num_rows = len(df)
        cols = tuple(df.columns)
        chunk_size = num_rows // 100
        with open(PATH_SQL + f'/stg.copy_{table}.sql', 'r') as script:
            script = script.read()
            with contextlib.closing(conn.cursor()) as cur:
                start = 0
                while start <= num_rows:
                    end = min(start + chunk_size, num_rows)
                    logger.info(f"loading rows {start}-{end}")    
                    df.loc[start: end].to_csv('/tmp/chunk.csv', index=False)
                    with open('/tmp/chunk.csv', 'rb') as chunk:
                        cur.copy(script.format(', '.join(cols)), chunk, buffer_size=65536)
                    conn.commit()
                    logger.info("loaded")
                    start += chunk_size + 1

class Loader:

    def __init__(self, date: str, pg_origin: PgConnect, vertica_dest: VerticaConnect, table: str, logger: Logger):
        self._date = datetime.fromisoformat(date + ' 00:00:00')
        self._origin = OriginRepository(pg_origin)
        self._stg = DestRepository()
        self._vertica_dest = vertica_dest
        self._table = table
        self._logger = logger


    def load(self):
        with self._vertica_dest.connection() as conn:
            load_data = self._origin.get_df(self._date, self._table)
            self._logger.info(f"Found {len(load_data)} {self._table} to load.")
            if load_data.shape[0]==0:
                self._logger.info("Quiting.")
                return
            self._stg.df_insert(conn, load_data, self._table, self._logger)
            self._logger.info(f"Load finished.")