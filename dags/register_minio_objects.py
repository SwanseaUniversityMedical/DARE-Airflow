import datetime
import hashlib
import os.path
import logging
import pendulum
import psycopg2
import boto3
import json
from airflow import DAG
from airflow.operators.python import get_current_context, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.utils.minioevent import unpack_minio_event

with DAG(
    dag_id="register_minio_objects",
    schedule=None,
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "s3", "register"],
) as dag:

    # Makes this logging namespace appear immediately in airflow
    logging.info("DAG parsing...")

    def process_event(message):
        logging.info("Processing message!")
        logging.info(f"message={message}")

        context = get_current_context()
        ti = context['ti']

        event = unpack_minio_event(message)
        logging.info(f"event={event}")

        #Register eTag in postgres if not already there

        postgres_conn = BaseHook.get_connection('pg_conn')
        
        # Establish connection to PostgreSQL
        conn = psycopg2.connect(
            dbname=postgres_conn.schema,
            user=postgres_conn.login,
            password=postgres_conn.password,
            host=postgres_conn.host,
            port=postgres_conn.port
        )
        cur = conn.cursor()

        value_to_insert = int(event["etag"])
        cur.execute(f"INSERT INTO register (etag)
        SELECT '{value_to_insert}'
        WHERE NOT EXISTS (
            SELECT 1 FROM your_table_name WHERE column_name = '{value_to_insert}'
        )")
        conn.commit()
        cur.close()
        conn.close()


    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name="afregister",
        deferrable=datetime.timedelta(seconds=120),
        poke_interval=datetime.timedelta(seconds=1),
        retry_delay=datetime.timedelta(seconds=10),
        retries=999999999,
    )

    # If the consumer task fails and isn't restarted, restart the whole DAG
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id=dag.dag_id,
        trigger_rule=TriggerRule.ALL_DONE
    )

    consume_events >> restart_dag
