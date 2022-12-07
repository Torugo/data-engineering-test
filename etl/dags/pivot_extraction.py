
from airflow.utils.dates import days_ago
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
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

    df_parquet = pd.read_parquet('/root/airflow/data/data-lake/bronze/raw.parquet')
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
    df_parquet['year_month'] = df_parquet.apply(lambda row: str(row['ano']) + '_' + row['mes'], axis=1)

    if validate_data(df_parquet):
        df_oil = df_parquet[df_parquet['product'] == 'ÓLEO_DIESEL']
        df_derivate =  df_parquet[df_parquet['product'] != 'ÓLEO_DIESEL']

        from pathlib import Path
        dest_dir = Path('/root/airflow/data/data-lake/silver')    
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        
        df_derivate = df_derivate.drop(['ano', 'mes', 'TOTAL'], axis=1)
        df_oil = df_oil.drop(['ano', 'mes', 'TOTAL'], axis=1)


        df_derivate.to_parquet('dest_dir' + '/' + 'derivate.parquet', partition_cols=['product', 'year_month', ])
        df_oil.to_parquet('dest_dir' + '/' + 'oil.parquet', partition_cols=['product', 'year_month', ])
    
    else:
        print("error in data validation")
        return -1

def validate_data(df):
    import pandas as pd
    import math
    prods = df['product'].unique()
    estados = df['uf'].unique()
    anos = df['ano'].unique()

    errors = []
    for prod in prods:
        for estado in estados:
            for ano in anos:
                mask = (df['product'] == prod) & (df['uf'] == estado)  & (df['ano'] == ano)
                df_filterers = df[mask]
                total = df_filterers['TOTAL'].unique()[0]
                data_sum = df_filterers['volume'].sum()
                if math.isnan(data_sum) or math.isnan(total):
                    print(data_sum, type(data_sum))
                    print(total, type(total))
                    print('Wrong number in totalization')
                    break
                if round(total, 3) != round(data_sum, 3): #avoiding mantissa error
                    print(f'error in data parsing for {prod}, {estado}, {ano}. Parsed sum {data_sum}. Raw sum {total}')
                    errors.append((prod, estado, ano))

    if len(errors) > 0:
        return False
    else:
        return True

def save_data(**kwargs):
    import pandas as pd
    from pathlib import Path
    dest_dir = Path('/root/airflow/data/data-lake/bronze')
    file_name = '/root/airflow/data/vendas-combustiveis-m3.ods'
    df = pd.read_excel(file_name, sheet_name='DPCache_m3 -1')
    df = df[df.COMBUSTÍVEL != 'ETANOL HIDRATADO (m3)']
    df = df.drop('REGIÃO', axis=1)

    dest_dir.mkdir(parents=True, exist_ok=True)

    df.to_parquet(str(dest_dir) + "/"  +'raw.parquet')

with DAG(
    'values_etl_dag',
    default_args=default_args,
    description='values dag',
    schedule_interval='@monthly',
    start_date=days_ago(2),
    tags=[],
) as dag:
    data_url = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-estatisticos/de/vdpb/vendas-combustiveis-m3.xls'
    file_name = data_url.split('/')[-1]
    download = BashOperator(task_id = 'download_pivot_data', bash_command=f"curl --create-dirs -O --output-dir /root/airflow/data/ {data_url}")
    ods_converter = BashOperator(task_id = 'convert_xls_ods', bash_command=f"soffice --headless --convert-to ods --outdir '/root/airflow/data/' '/root/airflow/data/{file_name}'  && test -s /root/airflow/data//vendas-combustiveis-m3.ods && exit 0 || exit -1")
    save = PythonOperator(task_id='save_data', python_callable=save_data)
    transform_and_save = PythonOperator(task_id='transform_save', python_callable=transform_save)

    download >> ods_converter >> save >> transform_and_save 