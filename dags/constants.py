rabbitmq_queue_minio_event='afload'
rabbitmq_queue_object_event='afobjectload'
rabbitmq_queue_minio_register='afregister'
rabbitmq_exchange_notify='notify'
rabbitmq_exchange_notify_key_s3file='s3'
rabbitmq_exchange_notify_key_trino='hive'
rabbitmq_exchange_notify_key_trino_iceberg='iceberg'

assets3_url = 'https://cat-hdp.demo.ukserp.ac.uk/doc/GetFilteredData2?profile=dlm&Filter=%22Dataset='

sql_trackingtable='''                
CREATE TABLE IF NOT EXISTS trackingtable (
            id VARCHAR(150), 
            dataset VARCHAR(150), 
            version VARCHAR(150), 
            label VARCHAR(150), 
            dated timestamp,
            bucket VARCHAR(150),
            key VARCHAR(150), 
            tablename VARCHAR(150), 
            physical VARCHAR(200)
        );
        '''

sql_tracking='''                
CREATE TABLE IF NOT EXISTS tracking (
            id VARCHAR(150), 
bucket VARCHAR(100),
path VARCHAR(500),
s_marker timestamp,
e_marker timestamp,
d_marker INT,
dataset VARCHAR(100),
version VARCHAR(100),
label VARCHAR(200),
filesize BIGINT,
filesize_par BIGINT,
columns INT,
s_download timestamp,
e_download timestamp,
d_download INT,
s_convert timestamp,
e_convert timestamp,
d_convert INT,
s_par timestamp,
e_par timestamp,
d_par INT,
s_upload timestamp,
e_upload timestamp,
d_upload INT,
s_schema timestamp,
e_schema timestamp,
d_schema INT,
s_hive timestamp,
e_hive timestamp,
d_hive INT,
s_iceberg timestamp,
e_iceberg timestamp,
d_iceberg INT
        );
        '''