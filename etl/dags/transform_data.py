
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
    def get_timestamp():
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc) # use POSIX epoch
        posix_timestamp_millis = (now - epoch) // timedelta(milliseconds=1)
        return posix_timestamp_millis

    import pandas as pd
    from pathlib import Path
    dest_dir = Path('/root/airflow/data/data-lake')

    df_parquet = pd.read_parquet('/root/airflow/data/data-lake/raw.parquet')
    df_parquet = df_parquet.fillna(0)
    df_parquet = df_parquet.rename(columns={"COMBUSTÍVEL": "product", "ESTADO": "uf", "UNIDADE": "unit", "ANO": 'ano'})
    df_parquet = pd.melt(df_parquet,
                            id_vars=['product', 'ano', 'unit', 'uf', 'TOTAL'],
                            var_name='mes',
                            value_name='volume') 

    df_parquet['product'] = df_parquet['product'].str.replace('m3', '', regex=True) 
    df_parquet['product'] = df_parquet['product'].str.replace('\(', '', regex=True) 
    df_parquet['product'] = df_parquet['product'].str.replace('\)', '', regex=True) 
    df_parquet['product'] = df_parquet['product'].str.rstrip().str.lstrip()
    df_parquet['product'] = df_parquet['product'].str.replace(' ', '_', regex=True) 

    df_parquet['mes'] = df_parquet['mes'].replace({'Jan': '01', 'Fev': '02', 'Mar': '03',
                                                'Abr': '04', 'Mai': '05', 'Jun': '06',
                                                'Jul': '07', 'Ago': '08', 'Set': '09',
                                                'Out': '10', 'Nov': '11', 'Dez': '12'})
    df_parquet['uf'] = df_parquet['uf'].replace({"ACRE":"AC",
                                            "ALAGOAS":"AL",
                                            "AMAZONAS":"AM",
                                            "AMAPÁ":"AP",
                                            "BAHIA":"BA",
                                            "CEARÁ":"CE",
                                            "DISTRITO FEDERAL":"DF",
                                            "ESPÍRITO SANTO":"ES",
                                            "GOIÁS":"GO",
                                            "MARANHÃO":"MA",
                                            "MINAS GERAIS":"MG",
                                            "MATO GROSSO DO SUL":"MS",
                                            "MATO GROSSO":"MT",
                                            "PARÁ":"PA",
                                            "PARAÍBA":"PB",	
                                            "PERNAMBUCO":"PE",
                                            "PIAUÍ":"PI",
                                            "PARANÁ":"PR",
                                            "RIO DE JANEIRO":"RJ",
                                            "RIO GRANDE DO NORTE":"RN",
                                            "RONDÔNIA":"RO",
                                            "RORAIMA":"RR",
                                            "RIO GRANDE DO SUL":"RS",
                                            "SANTA CATARINA":"SC",
                                            "SERGIPE":"SE",
                                            "SÃO PAULO":"SP",
                                            "TOCANTINS":"TO"})
    df_parquet['timestamp'] = get_timestamp()

    df_oil = df_parquet[df_parquet['product'] == 'ÓLEO_DIESEL']
    df_derivate =  df_parquet[df_parquet['product'] != 'ÓLEO_DIESEL']

    from pathlib import Path
    dest_dir = Path('/root/airflow/data/data-lake/silver')    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    ##VALIDATION

    df_derivate = df_derivate.drop(['ano', 'mes', 'TOTAL'], axis=1)
    df_oil = df_oil.drop(['ano', 'mes', 'TOTAL'], axis=1)


    df_derivate.to_parquet('dest_dir' + '/' + 'derivate.parquet', partition_cols=['product', 'year_month', ])
    df_oil.to_parquet('dest_dir' + '/' + 'oil.parquet', partition_cols=['product', 'year_month', ])


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