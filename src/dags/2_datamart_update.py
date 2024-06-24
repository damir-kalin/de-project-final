from datetime import datetime
import logging

from airflow import DAG
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.operators.dummy_operator import DummyOperator

VERTICA_CONNECTION = "VERTICA_CONNECTION"
PATH_SQL = "/lessons/dags/sql"

business_dt = '{{ ds }}'

log = logging.getLogger(__name__)

def read_sql_file(file_name: str) -> str:
    with open(PATH_SQL + '/' + file_name, 'r') as script:
        return script.read()


args = {
    "owner": "damir_sibgatov",
    'email': ['sibgatov.damir@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

dag = DAG(
        'import_data_to_dwh',
        default_args=args,
        description='Import to dwh',
        catchup=True,
        start_date=datetime(2022, 10, 2),
        end_date=datetime(2022, 11, 2),
        schedule_interval='@daily',
        max_active_runs = 1
) 

load_h = []
for h in ['h_account', 'h_operation', 'h_status', 'h_currency']:
    load_h.append(VerticaOperator(
        task_id = f'load_{h}',
        sql = read_sql_file(f'dwh.insert_{h}.sql'),
        vertica_conn_id = VERTICA_CONNECTION,
        queue='default',
        dag = dag
        )
    )

dm = DummyOperator(
    task_id = 'dummy_1',
    trigger_rule="all_success"
)

load_l = []
for l in ['s_operation', 'l_convertation_currency', 'l_operation_status', 'l_transaction']:
    load_l.append(VerticaOperator(
        task_id = f'load_{l}',
        sql = read_sql_file(f'dwh.insert_{l}.sql'),
        vertica_conn_id = VERTICA_CONNECTION,
        queue='default',
        dag = dag
        )
    )

global_metrics = VerticaOperator(
    task_id = 'global_metrics',
    sql = read_sql_file('dwh.insert_global_metrics.sql'),
    vertica_conn_id = VERTICA_CONNECTION,
    queue='default',
    trigger_rule="all_success",
    dag = dag
)


load_h >> dm >> load_l >> global_metrics