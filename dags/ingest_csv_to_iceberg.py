from datetime import timedelta
import hashlib
import logging
import time
from dags.modules.convert.get_instructions import get_instructions
from dags.modules.convert.ingest_csv_to_iceberg import ingest_csv_to_iceberg
import pendulum
import codecs
import chardet
from random import randint
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

import constants

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.databases.duckdb import s3_csv_to_parquet
from modules.utils.s3 import s3_download
from modules.utils.minioevent import unpack_minio_event, decode_minio_event
from modules.utils.version import compute_params


def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def sha1(value):
    sha_1 = hashlib.sha1()
    sha_1.update(str(value).encode('utf-8'))
    return sha_1.hexdigest()

def is_utf8(data):
    result = chardet.detect(data)
    print(f'Encoding determined to be {result}')
    return result['encoding'] == 'utf-8'


def convert_to_utf8(input_path, output_path):
    with open(input_path, 'rb') as input_file:
        with codecs.open(output_path, 'w', encoding='utf-8') as output_file:
            for line in input_file:
                try:
                    decoded_line = line.decode('utf-8')
                except UnicodeDecodeError:
                    decoded_line = line.decode('iso-8859-1')
                output_file.write(decoded_line)


# GET instructions for Assetsv3 on how to handle this dataset
def tracking_timer(p_conn, etag, variablename, tstart=time.time()):
    
    if str(variablename).startswith('s'):
        diff = 0    
        whichmarker = str(variablename).replace('s_','d_')
    else:
        enddiff = time.time()
        diff =  enddiff - tstart
        whichmarker = str(variablename).replace('e_','d_')

    with p_conn.cursor() as cur:
        sql = f"UPDATE tracking SET {variablename}=NOW(), {whichmarker}={diff} WHERE id = '{etag}' "
        cur.execute(sql)
    p_conn.commit()
    return time.time()

def tracking_data(p_conn, etag, variablename, data):
    with p_conn.cursor() as cur:
            sql = f"UPDATE tracking SET {variablename}={data} WHERE id = '{etag}' "
            cur.execute(sql)
    p_conn.commit()


    ########################################################################


def process_s3_object(bucket, key, etag):
    
    event = decode_minio_event(bucket, key, etag)
    logging.info(f"unpacked event={event}")

    # Based on the datasetname go and get an defined rules
    attribs, templates, duckdb_params, process, action = get_instructions(event['dir_name'])
    logging.info(f'attributes = {attribs}')
    logging.info(f'templates = {templates}')
    logging.info(f'process = {process}')
    logging.info(f'action = {action}')
    logging.info(f'duckdb param = {duckdb_params}')

    if process == "yesAlways":
            
        # Compute paarmeters based on data and any templates defined
        params = compute_params(event,attribs,templates)
        logging.info(f"Computed Params = {params}")

        ingest_csv_to_iceberg(dataset=params['dataset'],  
                            tablename=params["tablename"],  
                            version=params["version"],  
                            label=params["label"],
                            etag = event['etag'],
                            ingest_bucket=event['bucket'],
                            ingest_key=event['src_file_path'],
                            dag_id=event['etag']+str(random_with_N_digits(4)),
                            ingest_delete=False,
                            duckdb_params=duckdb_params,
                            action=action,
                            debug=True)
    else:
        logging.info("Instructions to abort processing")



with DAG(
    dag_id="ingest_csv_to_iceberg",
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

        # Process minio message into structure (two methods separated so the first one can be reused)
        bucket, key, etag = unpack_minio_event(message)
        logging.info(f'unpacked basic = bucket: {bucket}, key: {key}, etag: {etag}')

        # NOTE: done this way as manual trigger could be on another DAG listening to RabbitMQ with message that only has these three variables
        process_s3_object(bucket, key, etag)
        
    consume_events = RabbitMQPythonOperator(
        func=process_event,
        task_id="consume_events",
        rabbitmq_conn_id="rabbitmq_conn",
        queue_name=constants.rabbitmq_queue_minio_event,
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
