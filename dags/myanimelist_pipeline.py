from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import papermill as pm
import shutil

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def run_notebook():
    pm.execute_notebook(
        input_path='/workspace/notebooks/load_curated_tables.ipynb',
        output_path='/workspace/notebooks/load_curated_tables_output.ipynb',
        kernel_name='python3'
    )

with DAG(
    dag_id='myanimelist_pipeline',
    default_args=default_args,
    description='Monthly MAL data pipeline',
    schedule_interval='@monthly',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['myanimelist'],
) as dag:

    fetch_raw = BashOperator(
        task_id='fetch_raw_json',
        bash_command='cd /workspace && python -c "import sys; sys.path.append(\'/workspace/python\'); from fetch_raw_json import fetch_new_importbatch; fetch_new_importbatch()"',
    )

    load_bronze = BashOperator(
        task_id='load_bronze',
        bash_command='cd /workspace && python -c "import sys; sys.path.append(\'/workspace/python\'); from fetch_raw_json import load_missing_batches; load_missing_batches()"',
    )

    load_curated = PythonOperator(
        task_id='load_curated_tables',
        python_callable=run_notebook,
    )

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /workspace/dbt && dbt run',
    )

    copy_db = BashOperator(
        task_id='copy_duckdb',
        bash_command="python /workspace/python/datamart_export_to_file.py",
    )

    fetch_raw >> load_bronze >> load_curated >> run_dbt >> copy_db