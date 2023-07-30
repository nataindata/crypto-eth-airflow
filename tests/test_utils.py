import io
from dags.utils.utils import from_gcs_to_postgres, PG_SCHEMA
from unittest.mock import MagicMock, call, patch


@patch('psycopg2.connect')
@patch('urllib.request.urlopen')
@patch('airflow.hooks.base.BaseHook.get_connection')
def test_from_gcs_to_postgres_without_existing_data(mock_get_connection, mock_urlopen, mock_connect, caplog):
    # Mock the connection object
    conn_mock = MagicMock()
    cursor_mock = MagicMock()
    conn_mock.cursor.return_value = cursor_mock
    conn_mock.commit.return_value = None
    mock_connect.return_value = conn_mock
    mock_get_connection.return_value = conn_mock

    # Mock the response from the file URL
    mock_response = io.StringIO("from_address,to_address,value,transaction_hash,log_index,block_timestamp,block_number,block_hash\n"
                                "0x123456,0x789012,100,0xabcdef,1,2023-05-01 00:00:00,1,0x00000001\n"
                                "0x456789,0xdefabc,50,0x123456,2,2023-05-01 00:00:00,1,0x00000001\n")
    mock_urlopen.return_value.__enter__.return_value = mock_response

    # Mock the result of the SELECT query for existing data
    cursor_mock.fetchall.return_value = []

    # Call the function
    from_gcs_to_postgres("20230501", "token_transfers")

    # Assert that the correct SQL queries were executed
    expected_calls = [
        call(f"""BEGIN;\n        DELETE FROM {PG_SCHEMA}.token_transfers WHERE block_timestamp IN (SELECT block_timestamp FROM {PG_SCHEMA}.token_transfers_temp);\n        INSERT INTO {PG_SCHEMA}.token_transfers SELECT * FROM {PG_SCHEMA}.token_transfers_temp;\n    COMMIT;\n\n    DROP TABLE {PG_SCHEMA}.token_transfers_temp;\n    """),
    ]
    # Assert that the connection was closed and logging was done
    assert cursor_mock.execute.mock_calls == expected_calls
    assert caplog.records[0].message == f"{PG_SCHEMA}.token_transfers is updated"
