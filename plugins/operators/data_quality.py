from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id


    def execute(self, context):
        self.log.info('Running data quality checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records_check_sql = f"SELECT COUNT(*) FROM {table};"
            self.log.info(f"Running records check for table {table}")
            records = redshift.get_records(records_check_sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")

            num_records = records[0][0]
            if num_records == 0:
                raise ValueError(f"Data quality check failed. {table} contains 0 records")

            self.log.info(f"Data quality check passed for table {table} with {num_records} records.")

