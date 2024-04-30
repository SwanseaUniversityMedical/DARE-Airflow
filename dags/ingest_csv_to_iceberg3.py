from datetime import datetime, timedelta
import hashlib
import os
import os.path
import logging
import requests
import time
import pendulum
import pyarrow.parquet as pq
import json
import s3fs
import codecs
import chardet
import psycopg2
from random import randint
from airflow import DAG
from airflow.operators.python import get_current_context
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

import constants

from modules.providers.operators.rabbitmq import RabbitMQPythonOperator
from modules.databases.duckdb import s3_csv_to_parquet
from modules.databases.duckdb import file_csv_to_parquet
from modules.utils.s3 import s3_delete
from modules.utils.s3 import s3_create_bucket
from modules.utils.s3 import s3_download
from modules.utils.s3 import s3_download_minio
from modules.utils.s3 import s3_upload
from modules.utils.minioevent import unpack_minio_event, decode_minio_event
from modules.utils.version import attribute_search, compute_params
from modules.utils.rabbit import send_message_to_rabbitmq
from modules.databases.trino import (
    create_schema,
    drop_table,
    get_trino_conn_details,
    get_trino_engine,
    hive_create_table_from_parquet,
    iceberg_create_table_from_hive,
    validate_identifier,
    validate_s3_key
)


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
def get_instructions(datasetname):
    
    # need to compute this
    url =  constants.assets3_url + datasetname
    logging.info(f'Getting loading instructions from {url}')

    # templates and default values if not changed
    templates = dict(
        version_template=r'''{% if (s3.version) and s3.version %}{{ s3.version}}{% else %}{{ attrib["att_version"][0] }}{% endif %}''',
        label_template = '{{ s3.filename }}',
        table_template = '{{ attrib["label"] }}',
        dataset_template = '{{ s3.dir_name }}'
    )

    attribs = dict()
    duckdb_params = 'sample_size=-1,  ignore_errors=true'
    process = "yesAlways"
    action = "default"

    try:
        proxy = {
            'http': 'http://192.168.10.15:8080',
            'https': 'http://192.168.10.15:8080'
        }
        # Fetch JSON data from the URL and parse it into a Python variable
        #response = requests.get(url, proxies=proxy)
        response = requests.get(url)
        
        # Check if the response status code is OK (200)
        if response.status_code == 200:
            r_data = response.json()
            logging.info(r_data)

            hits = r_data.get("hits").get("total").get("value")
            logging.info(f"search hits = : {hits}")

            if hits == 1:  # single answer

                data = r_data.get("hits").get("hits")[0].get("_source")
                logging.info(data)

                process = data.get("process")

                if data.get("tableName"):
                    templates['table_template']=data.get("tableName")
                if data.get("version"):
                    templates['version_template']=data.get("version")
                if data.get("label"):
                    templates['label_template']=data.get("label")            
                if data.get("dataset_template"):
                    templates['dataset_template']=data.get("dataset_template") 
                        
                ignore_errors = data.get("IgnoreErrors")   
                if ignore_errors == 'default':
                    duckdb_params=""
                elif ignore_errors == 'False':
                    duckdb_params="ignore_errors=false,"
                else:
                    duckdb_params="ignore_errors=true,"

                header = data.get("header") 
                if header == 'True':
                    duckdb_params = duckdb_params + ' header=true,'
                elif header == 'False':
                    duckdb_params = duckdb_params + ' header=false,'
                
                action = data.get("action")
                sampling = data.get("sampling")
                duckdb_params = duckdb_params + 'sample_size=' + str(sampling)

                logging.info(f"IgnoreErrors: {ignore_errors}")
                logging.info(f"header: {header}")
                logging.info(f"sampling: {sampling}")

                attributes = data.get("attributes")
                if attributes:
                    for attribute in attributes:                    
                        attribute_name = attribute.get("attributeName")
                        attribute_source = attribute.get("source")
                        attribute_regex = attribute.get("regex")
                        attribute_single = attribute.get("single")
                        attribs[attribute_name]= attribute_search(attribute_source,attribute_regex,attribute_single)
                else:
                    logging.info("No attributes found.")
            else:
                logging.error(f"Error should only have a single value back from URL, returned = {hits}")        
        else:
            logging.error(f"Failed to fetch JSON data. Status code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching JSON data: {e}")

    return attribs, templates, duckdb_params, process, action


def pyarrow_to_trino_schema(schema):
    trino_schema = []

    for field in schema:
        # Extract field name and data type
        field_name = field.name
        field_type = str(field.logical_type)
        logging.info(f"pyarrow schema: {field_name} is {field_type} ({field.physical_type})")

        # Convert PyArrow data type to Trino-compatible data type
        if field_type == 'int64':
            trino_type = 'BIGINT'
        elif field_type.startswith('Int'):
            if field.physical_type == 'INT64':
                trino_type = 'BIGINT'
            else:
                trino_type = 'INTEGER'
        elif field_type.startswith('Float'):
            trino_type = 'DOUBLE'
        elif field_type.startswith('String'):
            trino_type = 'VARCHAR'
        elif field_type == 'Object':
            trino_type = 'VARCHAR'
        elif field_type.startswith('Bool'):
            trino_type = 'BOOLEAN'
        elif field_type.startswith('Timestamp'):
            trino_type = 'TIMESTAMP'
        elif field_type.startswith('Date'):
            trino_type = 'DATE'
        elif field_type == 'None':
            trino_type = field.physical_type
        else:
            # Use VARCHAR as default for unsupported types
            trino_type = 'VARCHAR'

        # Append field definition to Trino schema
        trino_schema.append(f'{field_name} {trino_type}')

    return ',\n'.join(trino_schema)

 
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


def ingest_csv_to_iceberg(dataset, tablename, version, label, etag, ingest_bucket, ingest_key, dag_id, ingest_delete, duckdb_params, action, debug):

    ########################################################################
    # Key settings

    load_layer_bucket = "loading"
    load_layer_schema = "minio.par"  # database and schema

    base_layer_bucket = "working"
    base_layer_bucket_dataset_specific = True

    append_GUID = False

    # dataset is first folder
    if dataset == '':
        dataset = 'none'
    dataset = dataset.lower()

    # version will be folder between datasetname and file/object

    formatted_date = datetime.today().strftime("%Y%m%d")
    ########################################################################

    logging.info("Starting function ingest_csv_to_iceberg...")

    # Extract the Airflow context object for this run of the DAG
    context = get_current_context()
    logging.info(f"context={context}")

    # Extract the task instance object for handling XCOM variables
    ti = context['ti']
    logging.info(f"ti={ti}")

    # Extract the JSON dict of params for this run of the DAG
    conf = context['dag_run'].conf
    logging.info(f"conf={conf}")

    # Unique hashed name for this run of the DAG
    dag_hash = sha1(
        f"dag_id={ti.dag_id}/"
        f"run_id={ti.run_id}/"
        f"task_id={ti.task_id}"
    )
    logging.info(f"dag_hash={dag_hash}")

    ########################################################################

    postgres_conn = BaseHook.get_connection('pg_conn')

    # Establish connection to PostgreSQL
    p_conn = psycopg2.connect(
        dbname=postgres_conn.schema,
        user=postgres_conn.login,
        password=postgres_conn.password,
        host=postgres_conn.host,
        port=postgres_conn.port
    )
    

    ########################################################################

    logging.info("Validate inputs...")

    # debug = conf.get("debug", False)
    logging.info(f"debug={debug}")
    assert isinstance(debug, bool)

    # Path to the data file within the ingest bucket excluding the bucket name
    # ingest_key = conf.get("ingest_key", None)
    logging.info(f"ingest_key={ingest_key}")
    assert (
        (ingest_key is not None) and
        isinstance(ingest_key, str) and
        ingest_key.endswith(".csv")
    )

    ingest_path = os.path.dirname(ingest_key)
    ingest_file = os.path.basename(ingest_key)
    # ingest_bucket = "ingest"

    # ingest_delete = conf.get("ingest_delete", False)
    logging.info(f"ingest_delete={ingest_delete}")
    assert isinstance(ingest_delete, bool)

    # if using dataset buckets, check we have the target bucket
    if base_layer_bucket_dataset_specific:
        logging.info(f"Trying to create S3 Bucket : {dataset}")
        s3_create_bucket(conn_id="s3_conn", bucket=dataset)

    ingest = {
        "bucket": ingest_bucket,
        "key": ingest_key,
        "path": ingest_path,
        "file": ingest_file,
        "dataset": dataset,
        "delete": ingest_delete,
    }
    logging.info(f"ingest={ingest}")
    ti.xcom_push("ingest", ingest)

    # create tracking entry
    with p_conn.cursor() as cur:
        sql = f"INSERT INTO tracking (id, dataset, version, label, s_marker, bucket, path) SELECT '{etag}','{dataset}','{version}','{label}', NOW(), '{ingest_bucket}', '{ingest_key}' WHERE NOT EXISTS (SELECT 1 FROM tracking WHERE id = '{etag}' )"
        cur.execute(sql)
    p_conn.commit()
    x2 = time.time()

    hive_schema = load_layer_schema
    hive_table = validate_identifier(f"{hive_schema}.{dataset}_{dag_id}")
    hive_bucket = load_layer_bucket
    hive_dir = validate_s3_key(f"ingest/{dataset}/{dag_id}")
    hive_file, _ = os.path.splitext(ingest_file)
    hive_key = f"{hive_dir}/{hive_file}.parquet"
    hive_path = validate_s3_key(f"{hive_bucket}/{hive_dir}")
    hive = {
        "schema": hive_schema,
        "table": hive_table,
        "bucket": hive_bucket,
        "dir": hive_dir,
        "file": hive_file,
        "key": hive_key,
        "path": hive_path,
    }
    logging.info(f"hive={hive}")
    ti.xcom_push("hive", hive)

    iceberg_schema = f"iceberg.{dataset}"  # "iceberg.ingest"

    tablename_ext = ""
    #if version:
    #    tablename_ext = tablename_ext + f"_{version}"
    if append_GUID:
        tablename_ext = tablename_ext + f"_{dag_id}"
    tempTabName = f"{iceberg_schema}.{tablename}{tablename_ext}".replace("-","_")
    logging.info(f"proposed iceberg name = {tempTabName}")
    iceberg_table = validate_identifier(tempTabName)

    iceberg_bucket = base_layer_bucket
    iceberg_dir = validate_s3_key(f"{dataset}/{version}")
    if base_layer_bucket_dataset_specific:
        iceberg_bucket = dataset
        iceberg_dir = validate_s3_key(f"{version}")

    iceberg_path = validate_s3_key(f"{iceberg_bucket}/{iceberg_dir}/{tablename}")

    iceberg = {
        "schema": iceberg_schema,
        "table": iceberg_table,
        "bucket": iceberg_bucket,
        "dir": iceberg_dir,
        "path": iceberg_path,
    }
    logging.info(f"iceberg={iceberg}")
    ti.xcom_push("iceberg", iceberg)

    # Use streaming ?
    if False :
        ########################################################################
        x=tracking_timer(p_conn, etag, "s_par")

        # Use DUCKDB S3 to S3
        logging.info("Convert from ingest bucket CSV to loading bucket Parquet using DuckDB...")
        s3_csv_to_parquet(
            conn_id="s3_conn",
            src_bucket=ingest_bucket,
            src_key=ingest_key,
            dst_bucket=hive_bucket,
            dst_key=hive_key
        )

        tracking_timer(p_conn, etag, "e_par",x) 

        ########################################################################
    
    else:
    
        x = tracking_timer(p_conn, etag, "s_download")

        # Use DUCKDB, download, convert upload
        temp_dir = "/home/airflow/"

        print(f'Downloading object from S3 from {ingest_bucket} --> {ingest_key}')
        down_dest=temp_dir+ingest_file
        s3_download_minio("s3_conn", bucket_name=ingest_bucket, object_name=ingest_key, local_file_path=down_dest)

        tracking_timer(p_conn, etag, "e_download",x)
        tracking_data(p_conn,etag,'filesize', os.path.getsize(down_dest))
        x=tracking_timer(p_conn, etag, "s_convert")

        # DUCKDB needs UTF-8 files, so check
        #with open(down_dest, 'rb') as file:
        #    in_data = file.read()
        
        # Check if file is UTF-8 encoded
        print("Testing if file is UTF-8 encoded")
        with open(down_dest, 'rb') as file:
            testdata = file.read(5000)
            #if is_utf8(testdata):
            # removed for now as does not work - so always run - this needs to be sorted as waste of energy
            if False:
                print("File is already UTF-8 encoded. No conversion needed.")
            else:
                print("File is not UTF-8 encoded. Converting to UTF-8...")
                down_dest2 = down_dest.replace(temp_dir,temp_dir+'c-')
                convert_to_utf8(down_dest, down_dest2)
                os.remove(down_dest)
                down_dest = down_dest2
                print("Conversion complete. New file created:", down_dest)
            
        tracking_timer(p_conn, etag, "e_convert",x)
        x=tracking_timer(p_conn, etag, "s_par")

        print(f"DUCKDB convert to parquet of file {down_dest}")
        file_csv_to_parquet(src_file=down_dest, dest_file=down_dest+'.parquet', duckdb_params=duckdb_params )

        tracking_timer(p_conn, etag, "e_par",x) 
        tracking_data(p_conn,etag,'filesize_par', os.path.getsize(down_dest+'.parquet'))   
        x=tracking_timer(p_conn, etag, "s_upload")

        print(f"Uploading to S3 {hive_bucket} - {hive_key}")
        s3_upload("s3_conn",down_dest+'.parquet',hive_bucket,hive_key)

        tracking_timer(p_conn, etag, "e_upload",x)

        # remove local files
        os.remove(down_dest)
        os.remove(down_dest+'.parquet')
        
        send_message_to_rabbitmq('rabbitmq_conn',constants.rabbitmq_exchange_notify, constants.rabbitmq_exchange_notify_key_s3file,
                                 {"dataset":dataset,"version":version,"label":label,"dated":formatted_date,
                                  "s3bucket": hive_bucket, "s3key":hive_key})
        
    ########################################################################

    if ingest_delete:
        s3_delete(conn_id="s3_conn", bucket=ingest_bucket, key=ingest_key)

    ########################################################################

    x=tracking_timer(p_conn, etag, "s_schema")

    logging.info("Getting schema from the new PAR file")
    s3_conn = json.loads(BaseHook.get_connection("s3_conn").get_extra())

    fs = s3fs.S3FileSystem(
        endpoint_url=s3_conn["endpoint_url"],
        key=s3_conn["aws_access_key_id"],
        secret=s3_conn["aws_secret_access_key"],
        use_ssl=False
    )

    with fs.open(F"s3://{hive_bucket}/{hive_key}", "rb") as fp:
        schema: pq.lib.Schema = pq.ParquetFile(fp).schema
    trino_schema = pyarrow_to_trino_schema(schema)
    # logging.info(f"trino schema={trino_schema}")

    tracking_data(p_conn, etag, "columns",str(len(trino_schema)))
    tracking_timer(p_conn, etag, "e_schema",x)

    ########################################################################

    logging.info("Mounting PAR on s3 into Hive connector and copy to Iceberg...")
    x=tracking_timer(p_conn, etag, "s_hive")

    # Create a connection to Trino
    trino_conn = get_trino_conn_details()
    trino = get_trino_engine(trino_conn)

    logging.info(f"Create schema [{hive_schema}] in Hive connector...")
    create_schema(trino, schema=hive_schema, location=hive_bucket)

    logging.info(f"Create schema [{iceberg_schema}] in Iceberg connector...")
    create_schema(trino, schema=iceberg_schema, location=iceberg_bucket)

    try:
        logging.info("Create table in Hive connector...")
        hive_create_table_from_parquet(
            trino,
            table=hive_table,
            location=hive_path,
            schema=trino_schema
        )

        tracking_timer(p_conn, etag, "e_schema",x)

        send_message_to_rabbitmq('rabbitmq_conn',constants.rabbitmq_exchange_notify, constants.rabbitmq_exchange_notify_key_trino,
                                 {"dataset":dataset,"version":version,"label":label,"dated":formatted_date,
                                  "s3location":hive_path, "dbtable": hive_table})
        # "schema":schema  can not be json serialised, so need to sort that ???
        
        x=tracking_timer(p_conn, etag, "s_iceberg")

        logging.info("Create table in Iceberg connector...")
        iceberg_create_table_from_hive(
            trino,
            schema=iceberg_schema,
            table=iceberg_table,
            hive_table=hive_table,
            location=iceberg_path,
            action=action
        )

        tracking_timer(p_conn, etag, "e_iceberg",x)

        send_message_to_rabbitmq('rabbitmq_conn',constants.rabbitmq_exchange_notify, constants.rabbitmq_exchange_notify_key_trino_iceberg,
                                 {"dataset":dataset,"version":version,"label":label,"dated":formatted_date,
                                  "s3location":iceberg_path, "dbtable": iceberg_table})
    
    finally:
        if debug:
            logging.info("Debug mode, not cleaning up table in Hive connector...")
        else:
            logging.info("Cleanup table in Hive connector...")
            drop_table(trino, table=hive_table)

            logging.info("Cleanup data from Hive connector in s3...")
            # External location data is not cascade deleted on drop table
            s3_delete(
                conn_id="s3_conn",
                bucket=hive_bucket,
                key=hive_key
            )

    with p_conn.cursor() as cur:
        sql = f"INSERT INTO trackingtable (id, dataset,version,label,dated,bucket,key) SELECT '{etag}','{dataset}','{version}','{label}', NOW(), '{ingest_bucket}', '{ingest_key}' WHERE NOT EXISTS (SELECT 1 FROM trackingtable WHERE id = '{etag}' )"    
        print(sql)
        cur.execute(sql)
        sql = f"UPDATE trackingtable set tablename = '{iceberg_table}', physical = '{iceberg_path}' where id = '{etag}'"
        print(sql)
        cur.execute(sql)
    p_conn.commit() 

    ########################################################################
    # close postgres connection
    tracking_timer(p_conn, etag, "e_marker",x2)

    p_conn.close()
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
    dag_id="ingest_csv_to_iceberg3",
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
