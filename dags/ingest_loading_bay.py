from datetime import timedelta
import logging
import pendulum
import json

from random import randint
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from dags.modules.utils.tracking_timer import tracking_timer
from dags.modules.convert.process_s3_object import process_s3_object

import constants

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.databases.duckdb import s3_csv_to_parquet
from modules.utils.s3 import s3_download
from modules.utils.minioevent import unpack_minio_event



with DAG(
    dag_id="ingest_loading_bay",
    schedule="@once",
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "iceberg", "s3"],
) as dag:

    # Makes this logging namespace appear immediately in airflow
    logging.info("DAG parsing...")

    def process_event(message):
        logging.info("Processing message!")
        logging.info(f"message={message}")

        message_json = json.loads(message)
        bucket = message_json["bucket"]
        key = message_json["key"]
        etag = message_json["etag"]

        logging.info(f'message parts = bucket: {bucket}, key: {key}, etag: {etag}')

        process_s3_object(bucket, key, etag)
        
    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name=constants.rabbitmq_queue_object_event,
        deferrable=timedelta(seconds=120),
        poke_interval=timedelta(seconds=1),
        retry_delay=timedelta(seconds=10),
        retries=999999999,
    )

    create_tracking_task = PostgresOperator(
        task_id='create_tracking',
        postgres_conn_id='pg_conn',  #drop table if exists tracking ;
        sql=constants.sql_tracking,
        dag=dag,
    )

    create_tracking_table_task = PostgresOperator(
        task_id='create_tracking_table',
        postgres_conn_id='pg_conn',  #drop table if exists tracking ;
        sql=constants.sql_trackingtable,
        dag=dag,
    )
    
    # If the consumer task fails and isn't restarted, restart the whole DAG
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id=dag.dag_id,
        trigger_rule=TriggerRule.ALL_DONE
    )

    create_tracking_task >> create_tracking_table_task >> consume_events >> restart_dag
