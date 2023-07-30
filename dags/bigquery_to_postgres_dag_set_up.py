import logging
from os import path
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.gcs import gcs_object_is_directory
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from utils.utils import RAW_BUCKET, PG_SCHEMA, PG_TOKEN_TABLE, PG_TOKEN_COLUMNS_SCHEMA


DAG_ID = path.basename(__file__).replace(".pyc", "").replace(".py", "")

"""
Dag to set up the environment for the backfill & daily dags pulling data from BigQuery to Postgres.
Insert JSON params with start_date & query limit.
"""


default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        "start_date": '2023-04-01',
        "query_limit": 10
    },
}

dag = DAG(
    DAG_ID,
    start_date=datetime(2023, 5, 5),
    description="""Create GSC bucket & Postgres target table before launching the backfill & daily dags pulling. 
                   Insert JSON params with start_date & query limit""",
    schedule_interval='@once',
    default_args=default_args,
    catchup=False
)


def check_and_create_bucket(bucket_name: str):
    """
    Creating GCS bucket if it doesn't exist
    :param bucket_name: Name of the bucket to be checked
    """
    gcs_hook = GCSHook()
    bucket_exists = gcs_object_is_directory(f"gs://{bucket_name}")
    if not bucket_exists:
        gcs_hook.create_bucket(bucket_name)
        logging.info(f'Created bucket: {bucket_name}')
    else:
        logging.info(f'Bucket {bucket_name} already exists.')


create_gcs_bucket = PythonOperator(
    task_id='create_gcs_bucket',
    python_callable=check_and_create_bucket,
    op_kwargs={'bucket_name': RAW_BUCKET},
    dag=dag
)

create_postgres_table = SQLExecuteQueryOperator(
    task_id='create_postgres_table',
    conn_id="postgres_conn",
    sql=f"""CREATE SCHEMA IF NOT EXISTS {PG_SCHEMA};
            CREATE TABLE IF NOT EXISTS {PG_SCHEMA}.{PG_TOKEN_TABLE} {PG_TOKEN_COLUMNS_SCHEMA};""",
    autocommit=True,
    dag=dag
)

trigger_dag = TriggerDagRunOperator(
    task_id="trigger_bigquery_to_postgres_backfill",
    trigger_dag_id="bigquery_to_postgres_dag_backfill",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    conf={"triggering_dag": "bigquery_to_postgres_dag_set_up",
          "start_date": "{{ params.start_date }}",
          "query_limit": "{{ params.query_limit }}",
          },
)

create_gcs_bucket >> create_postgres_table >> trigger_dag
