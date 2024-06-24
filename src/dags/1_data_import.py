from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from lib.pg_connect import ConnectionBuilder as PGConnectionBuilder
from lib.vertica_connect import ConnectionBuilder as VerticaConnectionBuilder

from stg.loader import Loader

PG_CONNECTION = "PG_CONNECTION"
VERTICA_CONNECTION = "VERTICA_CONNECTION"

log = logging.getLogger(__name__)

origin_pg_connect = PGConnectionBuilder.pg_conn(PG_CONNECTION)
dwh_verrtica_connect = VerticaConnectionBuilder.vertica_conn(VERTICA_CONNECTION)

def pg_load(name, dt):
    loader = Loader(dt, origin_pg_connect, dwh_verrtica_connect, name, log)
    loader.load()

business_dt = '{{ ds }}'

args = {
    "owner": "damir_sibgatov",
    'email': ['sibgatov.damir@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

dag = DAG(
        'import_data_to_stg',
        default_args=args,
        description='Import to stg',
        catchup=True,
        start_date=datetime(2022, 10, 1),
        end_date=datetime(2022, 11, 1),
        schedule_interval='@daily',
        max_active_runs = 1
) 

pg_load_list = []
for name in ['transactions', 'currencies']:
    pg_load_list.append(PythonOperator(
        task_id = f"{name}_load",
        python_callable = pg_load,
        op_kwargs = {
            'name': name,
            'dt': business_dt
        },
        dag = dag)
    )

pg_load_list