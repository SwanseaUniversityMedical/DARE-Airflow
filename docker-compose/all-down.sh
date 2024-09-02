cd ./airflow
docker compose down
cd ../minio-trino
docker compose down
cd ..
docker ps -a