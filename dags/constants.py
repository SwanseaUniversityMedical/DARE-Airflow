rabbitmq_queue_minio_event='afload'
rabbitmq_queue_minio_register='afregister'
rabbitmq_exchange_notify='notify'
rabbitmq_exchange_notify_key_s3file='s3'
rabbitmq_exchange_notify_key_trino='hive'
rabbitmq_exchange_notify_key_trino_iceberg='iceberg'

sql_trackingtable='''                
CREATE TABLE IF NOT EXISTS trackingtable (
            id VARCHAR(50), 
            dataset VARCHAR(50), 
            version VARCHAR(50), 
            label VARCHAR(50), 
            dated timestamp,
            bucket VARCHAR(50),
            key VARCHAR(150), 
            tablename VARCHAR(150), 
            physical VARCHAR(200)
        );
        '''

sql_tracking='''                
CREATE TABLE IF NOT EXISTS tracking (
            id VARCHAR(50), 
bucket VARCHAR(50),
path VARCHAR(500),
s_marker timestamp,
e_marker timestamp,
d_marker INT,
dataset VARCHAR(100),
version VARCHAR(50),
label VARCHAR(50),
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