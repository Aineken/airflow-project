from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table = table

    def execute(self, context):
        self.log.info('LoadDimensionOperator in proccess')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Fetching column names for {self.table}")
        query = f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{self.table}'
                    ORDER BY ordinal_position;
                """
        columns = redshift.get_records(query)
        column_list = ", ".join([col[0] for col in columns])

        # Prepare the insert statement dynamically
        self.log.info(f"Columns in table {self.table}: {column_list}")
        dim_table_sql = f"INSERT INTO {self.table} ({column_list})"
        dim_table_sql+=self.sql

        self.log.info(dim_table_sql)
        redshift.run(dim_table_sql)

