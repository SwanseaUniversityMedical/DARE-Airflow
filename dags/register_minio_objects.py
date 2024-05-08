import datetime
import logging
import pendulum
import psycopg2
import constants
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.utils.minioevent import unpack_minio_event

with DAG(
    dag_id="register_minio_objects",
    schedule="@once",
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

        bucket, key, etag = unpack_minio_event(message)

        # Register eTag in postgres if not already there

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
        
        logging.info(f"register adding eTag = {etag}")

        sql = f"INSERT INTO register (etag) SELECT '{etag}'  WHERE NOT EXISTS (SELECT 1 FROM register WHERE etag = '{etag}' )"
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name=constants.rabbitmq_queue_minio_register,
        deferrable=datetime.timedelta(seconds=120),
        poke_interval=datetime.timedelta(seconds=1),
        retry_delay=datetime.timedelta(seconds=10),
        retries=999999999,
    )

    create_register_table_task = PostgresOperator(
        task_id='create_register_table',
        postgres_conn_id='pg_conn',
        sql='''
        CREATE TABLE IF NOT EXISTS register (
            etag VARCHAR(100)
        );
        ''',
        dag=dag,
    )

    # If the consumer task fails and isn't restarted, restart the whole DAG
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id=dag.dag_id,
        trigger_rule=TriggerRule.ALL_DONE
    )

    create_register_table_task >> consume_events >> restart_dag
