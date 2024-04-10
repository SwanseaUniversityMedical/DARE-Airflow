import datetime
import hashlib
import logging
import pendulum
import json
import re
import polars as pl
import pyarrow
import s3fs
import pyarrow.csv as pcsv

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.operators.python import get_current_context

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

    src_file_path: str = s3_object["key"].replace('%2F', '/')
    assert src_file_path.endswith(".csv")

    file_name = src_file_path.replace(".csv", "")
    dir_name = src_file_path.split("/")[0]

    full_file_path = message_json['Key']
    head_path = '/'.join(full_file_path.split('/')[:-1])

    return dict(
        user=user,
        bucket=bucket,
        src_file_path=src_file_path,
        etag=etag,
        file_name=file_name,
        dir_name=dir_name,
        full_file_path=full_file_path,
        head_path=head_path
    )


def sha1(value):
    sha_1 = hashlib.sha1()
    sha_1.update(str(value).encode('utf-8'))
    return sha_1.hexdigest()


with DAG(
    dag_id="ingest_csv_to_par_polas",
    schedule=None,
    start_date=pendulum.datetime(1900, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    concurrency=1,
    tags=["ingest", "csv", "parquet", "s3"],
) as dag:

    # Makes this logging namespace appear immediately in airflow
    logger.info("DAG parsing...")

    def process_event(message):
        logger.info("Processing message!")
        logger.info(f"message={message}")

        context = get_current_context()
        ti = context['ti']
        dag_hash = sha1(
            f"dag_id={ti.dag_id}/"
            f"run_id={ti.run_id}/"
            f"task_id={ti.task_id}"
        )

        event = unpack_minio_event(message)
        logger.info(f"event={event}")

        trino_conn = get_trino_conn_details()
        trino_engine = get_trino_engine(trino_conn)

        # make schema to read the csv files to
        # location is not actually used since tables will be created
        # with external_location
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

        fullpath = str(event['full_file_path'])
        print(f'reading {fullpath}')


        with fs.open(fullpath, "rb") as fp:
            df = pl.scan_csv(fp)
#            filepath='/opt/airflow/logs/test.par'
#            df.write_parquet(filepath)

        schema = df.schema().with_names().into_iter().collect()
        print(schema)
        
    #  lk: https://docs.pola.rs/py-polars/html/reference/api/polars.scan_csv.html

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
    #restart_dag = TriggerDagRunOperator(
    #    task_id="restart_dag",
    #    trigger_dag_id=dag.dag_id,
    #    trigger_rule=TriggerRule.ALL_DONE
    #)

    consume_events # >> restart_dag
