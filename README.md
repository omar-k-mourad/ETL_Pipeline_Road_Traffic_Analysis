# Build ETL Data Pipelines with BashOperator using Apache Airflow

### setup airflow
```bash
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
