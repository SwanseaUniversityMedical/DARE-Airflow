import datetime
import logging
import pendulum
import json
import s3fs
import pyarrow.csv as pcsv

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.databases.trino import (
    get_trino_engine,
    get_trino_conn_details,
    trino_execute_query
)

logger = logging.getLogger(__name__)


def unpack_minio_event(message):
    import json

    logger.info("Decoding minio event as JSON")
    message_json = json.loads(message)

    records = message_json["Records"][0]

    s3_object = records["s3"]["object"]

    user = records["userIdentity"]["principalId"]
    bucket = records["s3"]["bucket"]["name"]
    etag = s3_object["eTag"]

    src_file_path: str = s3_object["key"]
    assert src_file_path.endswith(".csv")

    file_name = src_file_path.replace(".csv", "").replace('%2F', '_')

    full_file_path = message_json['Key']
    head_path = '/'.join(full_file_path.split('/')[:-1])

    return dict(
        user=user,
        bucket=bucket,
        src_file_path=src_file_path,
        etag=etag,
        file_name=file_name,
        full_file_path=full_file_path,
        head_path=head_path
    )


with DAG(
    dag_id="ingest_csv_to_parquet",
    schedule=None,
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "parquet", "s3"],
) as dag:

    def process_event(message):
        logger.info("Processing message!")
        logger.info(f"message={message}")

        event = unpack_minio_event(message)
        logger.info(f"event={event}")

        trino_conn = get_trino_conn_details()
        trino_engine = get_trino_engine(trino_conn)

        # make schema to read the csv files to
        trino_execute_query(trino_engine, '''
        CREATE SCHEMA IF NOT EXISTS 
        minio.load 
        WITH (
            location='s3a://loading/'
        )
        ''')

        # lazily compute csv schema directly from s3
        s3_conn = json.loads(BaseHook.get_connection("s3_conn").get_extra())

        fs = s3fs.S3FileSystem(
            endpoint_url=s3_conn["endpoint_url"],
            key=s3_conn["aws_access_key_id"],
            secret=s3_conn["aws_secret_access_key"],
            use_ssl=False
        )

        with fs.open(event['full_file_path'], "rb") as fp:
            schema = pcsv.open_csv(fp).schema.get_json()

        logger.info(f"schema={schema}")

        dtype_map = {
            "STRING": "VARCHAR"
        }

        # create a list of tuples of (name, dtype)
        # dtype is remapped if it is in the dtype_map
        minio_schema = [
            (
                field["name"],
                dtype_map.get(field["type"].upper(), field["type"].upper())
            )
            for field in schema["fields"]
        ]

        logger.info(f"schema json={schema}")

        # format the schema as sql
        minio_schema_str = ', '.join(map(lambda field: ' '.join(field), minio_schema))

        logger.info(f"schema str={schema}")

        # make a table pointing at csv in external location
        trino_execute_query(trino_engine, '''
        CREATE TABLE minio.load.{0} (
            {2}
        ) with (
            external_location = 's3a://{1}/',
            format = 'CSV'
        )
        '''.format(event['file_name'], event['head_path'], minio_schema_str))

        # make schema to read the parquet/iceberg files to
        q = '''create schema if not exists iceberg.sail with (location='s3a://working/')'''
        trino_execute_query(trino_engine, q)

        # SELECT FROM this table into the new one
        q = '''
        CREATE TABLE iceberg.sail.{0} WITH (
            location = 's3a://working/{1}/',
            format = 'PARQUET'
            )
        AS
            SELECT * 
            FROM minio.load.{0}
        '''.format(event['file_name'], event['head_path'])
        trino_execute_query(trino_engine, q)

    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name="airflow",
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
