from contextlib import contextmanager
from typing import Generator

import vertica_python
from airflow.hooks.base import BaseHook

class VerticaConnect:
    def __init__(self, host: str, port: str, schema:str, user: str, pw: str) -> None:
        self.host = host
        self.port = int(port)
        self.schema = schema
        self.user = user
        self.pw = pw

    @contextmanager
    def connection(self) -> Generator[vertica_python.Connection, None, None]:
        conn = vertica_python.connect(host=str(self.host),
                port=self.port,
                user=str(self.user),
                password=str(self.pw),
                db=str(self.schema))
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

class ConnectionBuilder:

    @staticmethod
    def vertica_conn(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)

        vertica = VerticaConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password))

        return vertica
