cd ./minio-trino
docker compose pull
docker compose up -d
cd ../airflow
docker compsoe pull
docker compose up -d
cd ..
docker ps -a