
from airflow.utils.dates import days_ago
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
}

with DAG(
    'values_etl_dag',
    default_args=default_args,
    description='values dag',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=[],
) as dag:
    data_url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls'
    file_name = data_url.split('/')[-1]
    download = BashOperator(task_id = 'download_pivot_data', bash_command=f"curl --create-dirs -O --output-dir /home/airflow/data/ {data_url}")
    ods_converter = BashOperator(task_id = 'convert_xls_ods', bash_command=f"soffice --headless --convert-to ods --outdir /home/airflow/data {file_name}", queue='libreoffice')
    # save = PythonOperator

    download >> ods_converter