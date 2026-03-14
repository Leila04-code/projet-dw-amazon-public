# dags/etl_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from extract   import extract
from transform import transform
from load      import load

default_args = {
    'owner'       : 'binome-a',
    'retries'     : 2,
    'retry_delay' : timedelta(minutes=5),
    'start_date'  : datetime(2024, 1, 1),
}

def run_extract(**kwargs):
    df = extract()
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

def run_transform(**kwargs):
    import pandas as pd
    raw = kwargs['ti'].xcom_pull(key='raw_data')
    df  = pd.read_json(raw)
    df_clean = transform(df)
    kwargs['ti'].xcom_push(key='clean_data', value=df_clean.to_json())

def run_load(**kwargs):
    import pandas as pd
    clean = kwargs['ti'].xcom_pull(key='clean_data')
    df    = pd.read_json(clean)
    df['date_id'] = pd.to_datetime(df['date_id'])
    load(df)

with DAG(
    dag_id          = 'etl_amazon_pipeline',
    default_args    = default_args,
    schedule_interval = '@daily',
    catchup         = False,
    description     = 'Pipeline ETL Amazon Sales Dataset'
) as dag:

    t1 = PythonOperator(task_id='extract',   python_callable=run_extract)
    t2 = PythonOperator(task_id='transform', python_callable=run_transform)
    t3 = PythonOperator(task_id='load',      python_callable=run_load)

    t1 >> t2 >> t3