import logging
import urllib.request
import psycopg2
from airflow.hooks.base_hook import BaseHook

START_DATE = '2023-04-01'
BQ_TOKENS_SOURCE = 'bigquery-public-data.crypto_ethereum.tokens'
BQ_TOKEN_TRANSFER_SOURCE = 'bigquery-public-data.crypto_ethereum.token_transfers'
BQ_PROJECT_NAME = 'crypto-eth-386208'
BQ_DB_NAME = 'stg_crypto'
BQ_STG_TABLE = 'stg_token_transfers'
RAW_BUCKET = 'raw_crypto_bucket'
PG_SCHEMA = 'crypto_eth'
PG_TOKEN_TABLE = 'token_transfers'
PG_TOKEN_COLUMNS_SCHEMA = '(token_address VARCHAR(50), symbol VARCHAR(50), name VARCHAR(50), decimals VARCHAR(50), ' \
                          'total_supply VARCHAR(50), from_address VARCHAR(50), to_address VARCHAR(50), ' \
                          'value VARCHAR(50), transaction_hash BYTEA, log_index INTEGER, block_timestamp TIMESTAMP, ' \
                          'block_number INTEGER, block_hash VARCHAR)'


def from_gcs_to_postgres(file_name: str, table: str, **kwargs):
    """
    Copy csv file from GCS into postgres token_transfers table,
    creating _temp staging table, inserting csv file,
    then inserting _temp into main tables. In case the dame date is found,
    dropping existing rows.

    :param file_name: Postfix of the csv file to get the data from. Could be "backfill" or timestamp
    :param table: Name of the table where the data should be loaded
    """
    conn_info = BaseHook.get_connection('postgres_conn')
    conn = psycopg2.connect(host=conn_info.host, database=conn_info.schema, port=conn_info.port,
                            user=conn_info.login, password=conn_info.password)
    cur = conn.cursor()
    file_url = f"https://storage.googleapis.com/{RAW_BUCKET}/token_transfers_{file_name}.csv"

    with urllib.request.urlopen(file_url) as file:
        cur.copy_expert(f"""
        CREATE TABLE {PG_SCHEMA}.{table}_temp {PG_TOKEN_COLUMNS_SCHEMA};
        COPY {PG_SCHEMA}.{table}_temp FROM STDIN WITH CSV HEADER;""", file)
        conn.commit()
    file.close()

    cur.execute(f"""BEGIN;
        DELETE FROM {PG_SCHEMA}.{table} WHERE block_timestamp IN (SELECT block_timestamp FROM {PG_SCHEMA}.{table}_temp);
        INSERT INTO {PG_SCHEMA}.{table} SELECT * FROM {PG_SCHEMA}.{table}_temp;
    COMMIT;

    DROP TABLE {PG_SCHEMA}.{table}_temp;
    """)
    conn.commit()
    cur.close()

    return logging.info(f"{PG_SCHEMA}.{table} is updated")
