# Build ETL Data Pipelines with BashOperator using Apache Airflow

This Apache Airflow DAG automates a simple ETL (Extract, Transform, Load) workflow for processing road traffic toll data. The pipeline begins by extracting the source files from a compressed archive, then extracts the required fields from CSV, TSV, and fixed-width text files. The extracted datasets are consolidated into a single CSV file, after which a transformation step standardizes the data by converting the vehicle type column to uppercase. Task dependencies ensure that each step executes in the correct order, demonstrating how Airflow can orchestrate sequential data engineering workflows using `BashOperator`.


### setup airflow inside docker container
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up airflow-init
```

### running airflow container
docker compose up

### Downloading data set
```bash
sudo curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -o [Destination]
```

