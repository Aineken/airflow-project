import pendulum
import os
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

dags_folder = conf.get('core', 'dags_folder')
sql_file = os.path.join(dags_folder,'create_tables.sql')
with open(sql_file, 'r') as f:
    sql_commands = f.read().split(';')




@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@weekly'
)

def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')
    end_operator = DummyOperator(task_id='End_execution')
    @task()
    def Create_tables():
        redshift_hook = PostgresHook("redshift")
        for sql_command in sql_commands:
            if isinstance(sql_command, str) and sql_command.strip():  # Ensure it's a string and not empty
                redshift_hook.run(sql_command)

    create_tables_task = Create_tables()


    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_path="s3://udacity-dend/log_data",
        s3_json_path='s3://udacity-dend/log_json_path.json',
        use_epochmillisecs = True
    )


    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_path="s3://udacity-dend/song_data/A/B"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        table='songplays'
    )


    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.user_table_insert,
        table='users'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.song_table_insert,
        table='songs'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.artist_table_insert,
        table='artists'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        sql=SqlQueries.time_table_insert,
        table='time'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=[ "songplays", "songs", "artists",  "time", "users"]
    )


    start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,
                             load_time_dimension_table] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
