from contextlib import contextmanager
from typing import Generator

import psycopg
from airflow.hooks.base import BaseHook


class PgConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)
# target_session_attrs=read-write
    def client(self):
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self.url())
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
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg
