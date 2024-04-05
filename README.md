# DARE-Airflow

Convert CSV to TRINO, using Airflow DAGS, messaged by rabbitmq from minio

## AREAS for imporovement
- Big issue with using DAH_ID+try number for temp table, if task fails over the try count resets so then table existis if using debug.  Might be better off with a GUID and the eTAG ?
- use extra settings from trino connection
- Create ledger rabbitMQ message uploading
- Handle recording of failed loading / conversion
- Dataset name and version - could come from S3 object TAG if set
- how to handle version
- handle re-loading (overite)
- optional to append GUID

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

example message

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