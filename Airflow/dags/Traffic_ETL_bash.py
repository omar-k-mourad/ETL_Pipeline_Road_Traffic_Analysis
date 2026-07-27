from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Paths
BASE_DIR = Path("/opt/airflow/dags/data")
STAGING_DIR = BASE_DIR / "staging"

# Default arguments
default_args = {
    "owner": "Omar",
    "email": ["omar.khaled.morad@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG Definition
with DAG(
    dag_id="ETL_toll_data",
    default_args=default_args,
    description="Apache Airflow Final Assignment",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",       
    catchup=False,
    tags=["etl", "final-assignment"],
) as dag:

    # Task 1: Unzip source files
    unzip_data = BashOperator(
        task_id="unzip_data",
        bash_command=f"""
        tar -xzvf {BASE_DIR}/tolldata.tgz \
        -C {BASE_DIR}
        """,
    )

    # Task 2: Extract data from CSV
    extract_data_from_csv = BashOperator(
        task_id="extract_data_from_csv",
        bash_command=f"""
        cut -d"," -f1-4 \
        {BASE_DIR}/vehicle-data.csv \
        > {BASE_DIR}/csv_data.csv
        """,
    )

    # Task 3: Extract data from TSV
    extract_data_from_tsv = BashOperator(
        task_id="extract_data_from_tsv",
        bash_command=f"""
        cut -f5-7 --output-delimiter=',' \
        {BASE_DIR}/tollplaza-data.tsv \
        | tr -d '\\r' \
        > {BASE_DIR}/tsv_data.csv
        """,
    )

    # Task 4: Extract data from fixed-width text
    extract_data_from_fixed_width = BashOperator(
        task_id="extract_data_from_fixed_width",
        bash_command=f"""
        cut -c60-64,65- \
        {BASE_DIR}/payment-data.txt \
        | awk '{{$1=$1}}1' OFS=',' \
        > {BASE_DIR}/fixed_width_data.csv
        """,
    )

    # Task 5: Consolidate extracted data
    consolidate_data = BashOperator(
        task_id="consolidate_data",
        bash_command=f"""
        paste -d ',' \
            {BASE_DIR}/csv_data.csv \
            {BASE_DIR}/tsv_data.csv \
            {BASE_DIR}/fixed_width_data.csv \
            > {BASE_DIR}/extracted_data.csv
        """,
    )


    # Task 6: Transform data
    transform_data = BashOperator(
        task_id="transform_data",
        bash_command=f"""
        awk -F ',' 'BEGIN{{OFS=","}} {{$4=toupper($4); print}}' \
            {BASE_DIR}/extracted_data.csv \
            > {STAGING_DIR}/transformed_data.csv
        """,
    )

    # Task Dependencies
    (
        unzip_data
        >> extract_data_from_csv
        >> extract_data_from_tsv
        >> extract_data_from_fixed_width
        >> consolidate_data
        >> transform_data
    )