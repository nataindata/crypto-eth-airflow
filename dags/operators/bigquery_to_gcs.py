from airflow.utils.decorators import apply_defaults
from airflow.providers.google.cloud.transfers.sql_to_gcs import BaseSQLToGCSOperator


# TODO refactor pipelines to use this operator
class BQueryToGCSOperator(BaseSQLToGCSOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def query(self, **kwargs):
        pass

    def get_schema(self, **kwargs):
        pass

    def convert_type(self, **kwargs):
        pass

    def field_to_bigquery(self, **kwargs):
        pass
