import datetime
import hashlib
import logging
import pendulum
import json
import re
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


def sha1(value):
    sha_1 = hashlib.sha1()
    sha_1.update(str(value).encode('utf-8'))
    return sha_1.hexdigest()


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

        with fs.open(event['full_file_path'], "rb") as fp:
            schema: pyarrow.lib.Schema = pcsv.open_csv(fp).schema

        logger.info(f"schema={schema}")

        # format the schema as sql
        # hive connector enforces VARCHAR when backed by csv files in bucket
        # we'll cast the columns to the inferred schema when going to iceberg
        hive_schema_str = ', '.join(
            map(lambda field: f"{re.sub(r'[^a-zA-Z0-9]', '_', field.name).strip().strip('_').strip()} VARCHAR", schema)
        )
        logger.info(f"hive table schema={hive_schema_str}")

        table_name = re.sub(r"[^a-zA-Z0-9]", '_', event['file_name']).strip().strip('_').strip()
        logger.info(f"table name={table_name}")

        hive_table_name = f"{table_name}_{dag_hash}_{ti.try_number}"
        logger.info(f"hive table name={hive_table_name}")

        # make a hive table pointing at csv in external location
        trino_execute_query(trino_engine, '''
        CREATE TABLE minio.load.{0} (
            {1}
        ) with (
            external_location = 's3a://{2}',
            format = 'CSV'
        )
        '''.format(hive_table_name, hive_schema_str, event['head_path']))

        # make schema to read the parquet/iceberg files to
        trino_execute_query(trino_engine, '''
        CREATE SCHEMA IF NOT EXISTS 
            iceberg.sail 
        WITH (
            location='s3a://working/'
        )''')

        # convert the pyarrow schema to a hive sql schema
        #TODO add other iceberg datatypes
        dtype_map = {
            "STRING": "VARCHAR"
        }

        # create a list of tuples of (name, dtype)
        # dtype is remapped if it is in the dtype_map
        field: pyarrow.lib.Field
        iceberg_schema = [
            (
                re.sub(r'[^a-zA-Z0-9]', '_', field.name).strip().strip('_').strip(),
                dtype_map.get(str(field.type).upper(), str(field.type).upper())
            )
            for field in schema
        ]

        # format the schema as sql for column-wise cast from hive table
        iceberg_schema_str = ', '.join(
            [f"CAST({name} AS {dtype}) AS {name}"
             for name, dtype in iceberg_schema]
        )
        logger.info(f"iceberg table schema={iceberg_schema_str}")

        iceberg_table_name = table_name
        logger.info(f"iceberg table name={iceberg_table_name}")

        # clear current table if it exists
        #TODO handle this safely, we should be ingesting into a holding table
        #     before running a GE and DBT pipeline to merge it with existing data
        trino_execute_query(trino_engine, '''
        DROP TABLE IF EXISTS iceberg.sail.{0}
        '''.format(iceberg_table_name))

        # SELECT FROM the hive table into the iceberg table
        trino_execute_query(trino_engine, '''
        CREATE TABLE iceberg.sail.{0} WITH (
            location = 's3a://working/{0}/',
            format = 'PARQUET'
            )
        AS
            SELECT {1} FROM minio.load.{0}
        '''.format(iceberg_table_name, iceberg_schema_str))

        # cleanup the hive table
        trino_execute_query(trino_engine, '''
        DROP TABLE IF EXISTS minio.load.{0}
        '''.format(hive_table_name))

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
