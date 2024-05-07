import dags.constants
from dags.ingest_csv_to_iceberg import convert_to_utf8, sha1, tracking_data, tracking_timer
from dags.modules.databases.duckdb import file_csv_to_parquet
from dags.modules.databases.trino import create_schema, drop_table, get_trino_conn_details, get_trino_engine, hive_create_table_from_parquet, iceberg_create_table_from_hive, validate_identifier, validate_s3_key
from dags.modules.utils.rabbit import send_message_to_rabbitmq
from dags.modules.utils.s3 import s3_create_bucket, s3_delete, s3_download_minio, s3_upload


import psycopg2


import json
import logging
import os
import os.path
import time
from datetime import datetime

from dags.modules.convert.pyarrow_to_trino_schema import pyarrow_to_trino_schema


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