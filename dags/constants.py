rabbitmq_queue_minio_event='afload'
rabbitmq_queue_object_event='afobjectload'
rabbitmq_queue_minio_register='afregister'

rabbitmq_exchange_load='load'
rabbitmq_exchange_load_key_s3file='s3'

rabbitmq_exchange_notify='notify'
rabbitmq_exchange_notify_key_s3file='s3'
rabbitmq_exchange_notify_key_trino='hive'
rabbitmq_exchange_notify_key_trino_iceberg='iceberg'

redis_expiry = 30

assets3_url = 'https://cat-hdp.demo.ukserp.ac.uk/doc/GetFilteredData2?profile=dlm&Filter=%22Dataset='

process_s3_option_load   = "load"
process_s3_option_manual = "manual"
process_s3_option_whatif = "whatif"

process_s3_formoption_yesauto = "yesAlways"
process_s3_formoption_yesmanual = "yesManual"
process_s3_formoption_no = "no"

sql_trackingtable='''               
CREATE TABLE IF NOT EXISTS trackingtable (
            id VARCHAR(150), 
            dataset VARCHAR(150), 
            version VARCHAR(150), 
            label VARCHAR(150), 
            dated timestamp,
            bucket VARCHAR(150),
            key VARCHAR(350), 
            tablename VARCHAR(150), 
            physical VARCHAR(200)
        );
        '''

sql_tracking='''                
CREATE TABLE IF NOT EXISTS tracking (
id VARCHAR(350), 
bucket VARCHAR(100),
path VARCHAR(500),
s_marker timestamp,
e_marker timestamp,
d_marker INT,
dataset VARCHAR(100),
version VARCHAR(100),
label VARCHAR(200),
schema_hive VARCHAR(150),
tablename_hive VARCHAR(200),
location_hive VARCHAR(300),
schema_ice VARCHAR(150),
tablename_ice VARCHAR(200),
location_ice VARCHAR(300),
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
d_iceberg INT,
params VARCHAR(1000)
        );
        '''
