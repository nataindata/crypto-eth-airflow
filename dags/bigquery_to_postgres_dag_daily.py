from os import path
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from bigquery_to_postgres_dag_set_up import RAW_BUCKET, PG_TOKEN_TABLE
from utils.utils import from_gcs_to_postgres, BQ_TOKEN_TRANSFER_SOURCE, BQ_TOKENS_SOURCE, BQ_PROJECT_NAME, BQ_DB_NAME, \
    BQ_STG_TABLE

DAG_ID = path.basename(__file__).replace(".pyc", "").replace(".py", "")
DS = '{{ ds }}'

"""
Dag to pull data from BigQuery to PostgreSQL with schedule_interval @daily.
With BigQueryExecuteQueryOperator pulling data from BigQuery target table into staging BQ table.
Then BigQueryToGCSOperator pulls data from staging BQ table into GCS bucket.
Finally PythonOperator pulls data from GCS bucket into PostgreSQL.
"""

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    DAG_ID,
    start_date=datetime(2023, 5, 5),
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


task_bq_to_stage_bq = BigQueryExecuteQueryOperator(
    task_id='task_bq_to_stage_bq',
    sql=f"""
        SELECT
           CAST(token_address as STRING) as token_address,
           CAST(symbol as STRING) as symbol,
           CAST(name as STRING) as name,
           CAST(decimals as STRING) as decimals,
           CAST(total_supply as STRING) as total_supply,
           CAST(from_address as STRING) as from_address,
           CAST(to_address as STRING) as to_address,
           CAST("value" as STRING) as eth_value,
           CAST(transaction_hash as STRING) as transaction_hash,
           CAST(log_index as INTEGER) as log_index,
           CAST(tt.block_timestamp as TIMESTAMP) as block_timestamp,
           CAST(tt.block_number as INTEGER) as block_number,
           CAST(tt.block_hash as STRING) as block_hash,
        FROM {BQ_TOKEN_TRANSFER_SOURCE} as tt
        JOIN {BQ_TOKENS_SOURCE} as t
        ON tt.token_address = t.address
        WHERE
        DATE(tt.block_timestamp) = '{DS}'
         AND token_address IS NOT NULL
         AND transaction_hash IS NOT NULL
         AND log_index IS NOT NULL
         AND tt.block_timestamp IS NOT NULL
         AND tt.block_number IS NOT NULL
         AND tt.block_hash IS NOT NULL
         AND token_address LIKE '0x%'
         AND tt.block_number > 0
         AND log_index > 0
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
        LIMIT { "{{ params.query_limit }}" }
    """,
    destination_dataset_table=f"{BQ_PROJECT_NAME}.{BQ_DB_NAME}.{BQ_STG_TABLE}",
    write_disposition='WRITE_TRUNCATE',
    use_legacy_sql=False,
    dag=dag
)

task_stage_gbq_to_gcs = BigQueryToGCSOperator(
    task_id='task_bq_to_gcs',
    source_project_dataset_table=f'{BQ_PROJECT_NAME}.{BQ_DB_NAME}.{BQ_STG_TABLE}',
    destination_cloud_storage_uris=[f'gs://{RAW_BUCKET}/token_transfers_{DS}.csv'],
    export_format='csv',
    print_header=False,
    dag=dag
)


task_gcs_to_postgres = PythonOperator(
    task_id='task_gcs_to_postgres',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    python_callable=from_gcs_to_postgres,
    op_args=[DS, PG_TOKEN_TABLE],
    provide_context=True,
    dag=dag)

task_bq_to_stage_bq >> task_stage_gbq_to_gcs >> task_gcs_to_postgres
