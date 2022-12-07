
from airflow.utils.dates import days_ago
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
default_args = {
    'owner': 'airflow',
}


def transform_save(**kwargs):
    import pandas as pd
    from pathlib import Path
    dest_dir = Path('/root/airflow/data/data-lake')
    file_name = '/root/airflow/data/vendas-combustiveis-m3.ods'
    df = pd.read_excel(file_name, sheet_name='DPCache_m3 -1')
    df = df[df.COMBUSTÍVEL != 'ETANOL HIDRATADO (m3)']
    df = df.drop('REGIÃO', axis=1)

    dest_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(str(dest_dir) + "/"  +'raw.parquet')

with DAG(
    'transform_etl_dag',
    default_args=default_args,
    description='values dag',
    schedule_interval='@monthly',
    start_date=days_ago(2),
    tags=[],
) as dag:

    raw_extract = ExternalTaskSensor('values_etl_dag', check_existence=True)

    # download = BashOperator(task_id = 'download_pivot_data', bash_command=f"curl --create-dirs -O --output-dir /root/airflow/data/ {data_url}")
    # ods_converter = BashOperator(task_id = 'convert_xls_ods', bash_command=f"soffice --headless --convert-to ods --outdir '/root/airflow/data/' '/root/airflow/data/{file_name}'  && test -s /root/airflow/data//vendas-combustiveis-m3.ods && exit 0 || exit -1")
    transform_and_save = PythonOperator(task_id='transform_save', python_callable=transform_save)

    raw_extract >> transform_and_save