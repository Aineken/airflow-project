from airflow.hooks.postgres_hook import PostgresHook
from airflow.secrets.metastore import MetastoreBackend
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'



    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_path="",
                 s3_json_path = "auto",
                 use_epochmillisecs = False,
                 *args, **kwargs):



        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.s3_json_path = s3_json_path
        self.use_epochmillisecs = use_epochmillisecs


    def execute(self, context):
        self.log.info('StageToRedshiftOperator in proccess')
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))



        copy_sql = f"""
                    COPY {self.table} 
                    FROM '{self.s3_path}' 
                    ACCESS_KEY_ID '{aws_connection.login}'
                    SECRET_ACCESS_KEY '{aws_connection.password}'
                    FORMAT AS JSON '{self.s3_json_path}'
                    compupdate off 
                    REGION 'us-west-2'
                """

        if self.use_epochmillisecs:
            copy_sql += " TIMEFORMAT 'epochmillisecs'"

        self.log.info(f"Executing COPY command: {copy_sql}")

        redshift.run(copy_sql)










