# DARE-Airflow

Convert CSV to TRINO, using Airflow DAGS, messaged by rabbitmq from minio

## Table of Contents
- [Overview](#overall-architecture)
- [Areas for Improvement](#areas-for-imporovement)
- [Airflow configuration](#areas-for-imporovement)
- [Trino Configuration](#trino-configuration)
- [Minio Configuration](#mino-configuration)
    - [Example Minio JSON message](#minio-example-message)
- more stuff ..



## Overall architecture

The over all architecture is

1. Data loaded into Minio, which has been configured to have a connection to RabbitMQ through its AQMP plugin.  This connector will create a JSON message describing the object in Minio ([Example](#minio-example-message)).  In Mino a bucket can have its events linked to this AQMP cpability and the approach taken is to define the PUT event on a bucket to fire a AQMP/RabbitMQ message. (currently this need to be defined manually)

2. The docker-compose creates the RabbitMQ exchange for Minio to send the events to, this Exchange is called "minio". 

3. The docker-compose defeines Two RabbitMQ queues
    1. afloading - used to process the data and laod into Trino
    2. afregister - used to register this object as have been "seen" in the database

4. An Airflow DAG called "ingest_csv_to_iceberg2" subscribes to the RabbitMQ queue "afloading" and is used to read the CSV file from Minio and convert it to parquet and thewn iceberg, while registering this as a table in Trino.  **it will** once the table exists in Trino a new RabbitMQ message is created to inform other processes of this table.

*add link or details to the setting and fact must end in CSV and be in a directory which is the datasets and possible define the version etc*

5. Conversion from CSV to parquet is done through
    1. DuckDB (used as a library inside the DAG), which can mount an S3 bucket to read CSV, with imputed schema, and then in a single operation write the results of a select * on the source CSV to a output table in S3 choosing Parquet as the table structure.  This approach seem really fast and given it must read ever row for the conversion seems to get the schema correct.  
    2. The resultant parquet file is then registered in Trino using a Create Table with external location approach, this requires the SQL statement to know the table schema, this is done by PYARROW reading the schema from the S3 parquet object and then converting from PYARROW table datatypes to Trino datatypes to construct the required SQL
    3. Using Trino the "hive" parquet registered Table to read and inserted into a ICEBERG equiverlant, with Trino doing the conversion from plain Parquet to Iceberg
    4. Optionally (flags) the original "hive" parquet table can then be removed
    5. Optionally (flags) the Iceberg tables can use S3 buckets per dataset (created by the system) or place all data int he save S3 Bucket ("working" bucket) in dataset folders.  The purpose of this is to enble the system to be used to load data into trino for actual use or to load into Trino for onward processing and manipulation.

6. An Airflow DAG called "register_minio_objects" subscribes to the RabbitMQ queue "afregister" and it is used to add this object to Postgres.  The purpose of this is to allow a module (**not yet created**) to periodically scan the Minio buckets and pick up any objects that where not picked up by the other DAG.
    - The reason for this approach is that the link between Minio and RabbitMQ has been show during development to occationally have issues in that Minio can not communicate with RabbitMQ.  Minio doe snot have a re-try capability and therefore should this happen the opertunity to ingest this file/object will have been lost.  Therefore if periodically we sweep the buckets and check against a "register" of what has and what has not been processed, we can fire addional events into RabbitMQ for the "missing" files/objects.

7. TODO
    1. send RabbitMQ for resultant table in trino
    2. capture this message and trun into NRDAv2 : Create Ledger
    3. Scan Minio periodically and detect "missed" files/objects
    4. Improve detection of errors
    5. capture table message to populate Assets 3 / DBT configuration YAML





## AREAS for imporovement
- (DONE) Big issue with using DAH_ID+try number for temp table, if task fails over the try count resets so then table existis if using debug.  Might be better off with a GUID and the eTAG ?
- use extra settings from trino connection
- Create ledger rabbitMQ message uploading
- Handle recording of failed loading / conversion
- Dataset name and version - could come from S3 object TAG if set
- how to handle version
- handle re-loading (overite)
- (DONE) optional to append GUID

## Airflow Configuration
Airflow needs 3 connections (s3/rabbitmq/trino)

### Rabbit MQ Connection
![RabbitMq Connection](./images/rabbitmq-conn.PNG)

### S3 Connection
![S3 connection](./images/s3-conn.PNG)

### Trino Connection
![Trino connection](./images/trino-conn.PNG)
**There is a issue with the code that the EXTRA part of the trino connection is currently hard coded and needs to be chanegd to use the setting in the connection**

### Trino Configuration
The docker-compose sets up trino and connects to the minio.  There is currently no additional configuration needed to stand up the dev enviroment.

The Code (DAG) will automatically create the required schemas etc

**The current configuration is not correctly wired up to use the "default" schema** Not required but somethign to tidy up



### Mino Configuration

The system currently needs TWO buckets
- ingest
- loading
- working

The docker-compose currently creates these buckets **no security is aplied at this point (dev enviroment)**

Access Keys also need creating and given to airflow so the running DAG can access Minio **manual task**

The docker-compose wires up the *MINIO_NOTIFY_AMQP_...* setting to create a link from minio to rabbitmq.  However a **manual** task is to tell minio to send an event upload a file/object being uploaded.  This is done by adding the subscription to the bucket (events tab), selecting the PUT operation.
![Mino event dialog box](./images/minio-event2.PNG)
![Minio events](./images/rminio-events.PNG) --> this should be on the **INGEST** bucekt

This will create a json message uplaod upload which will get sent tot he **minio** exchaneg on rabbit.  This exchange is created by docker-compose, however if not present then minio will create it.

The docker-compose creates a queue called **airflow** which binds to this exchange, with airflow obviously ingesting the messages fromt hsi queue

### Minio example message

    {
    "EventName":"s3:ObjectCreated:Put",
    "Key":"loading/PEDW/20230101/PEDW_DIAG2.csv",
    "Records":[
        {
            "eventVersion":"2.0",
            "eventSource":"minio:s3",
            "awsRegion":"",
            "eventTime":"2024-04-05T08:57:31.324Z",
            "eventName":"s3:ObjectCreated:Put",
            "userIdentity":{
                "principalId":"minio"
            },
            "requestParameters":{
                "principalId":"minio",
                "region":"",
                "sourceIPAddress":"172.23.0.1"
            },
            "responseElements":{
                "x-amz-id-2":"dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8",
                "x-amz-request-id":"17C3568E062A6AC0",
                "x-minio-deployment-id":"21c4d138-2f32-45b9-8481-066ee9b9fe81",
                "x-minio-origin-endpoint":"http://172.23.0.3:9000"
            },
            "s3":{
                "s3SchemaVersion":"1.0",
                "configurationId":"Config",
                "bucket":{
                "name":"loading",
                "ownerIdentity":{
                    "principalId":"minio"
                },
                "arn":"arn:aws:s3:::loading"
                },
                "object":{
                "key":"PEDW%2F20230101%2FPEDW_DIAG2.csv",
                "size":7070,
                "eTag":"1c7915281bcf29f9469def4bc9bc91c9",
                "contentType":"text/csv",
                "userMetadata":{
                    "content-type":"text/csv"
                },
                "sequencer":"17C3568E062F2AA7"
                }
            },
            "source":{
                "host":"172.23.0.1",
                "port":"",
                "userAgent":"MinIO (linux; amd64) minio-go/v7.0.69 MinIO Console/(dev)"
            }
        }
    ]
    }