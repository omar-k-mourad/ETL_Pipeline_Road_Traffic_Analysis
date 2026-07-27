# Build ETL Data Pipelines with BashOperator using Apache Airflow

### setup airflow
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
```

### running airflow container
docker compose up

### Downloading data set
sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o [Destination]

