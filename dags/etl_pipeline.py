# dags/etl_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/opt/airflow/scripts')

from extract   import extract
from transform import transform
from load      import load
from simulate_daily import simulate_daily_sales  # ← importer
default_args = {
    'owner'       : 'binome-a',
    'retries'     : 2,
    'retry_delay' : timedelta(minutes=5),
    'start_date'  : datetime(2026, 1, 1),
}
from simulate_daily import simulate_daily_sales  # ← importer
# ✅ Changer ici pour switcher test ↔ prod
MODE      = 'test'   # 'test' ou 'prod'
FILEPATH  = 'data/Amazon-test.csv' if MODE == 'test' else 'data/Amazon.csv'


def run_simulate(**kwargs):
    simulate_daily_sales(
        filepath='data/Amazon-test.csv')
    
def run_extract(**kwargs):
    df = extract(filepath=FILEPATH)
    if df.empty:
        # ✅ Stopper proprement si rien de nouveau
        kwargs['ti'].xcom_push(key='raw_data', value=None)
        return
    kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

def run_transform(**kwargs):
    import pandas as pd
    raw = kwargs['ti'].xcom_pull(key='raw_data')
    if raw is None:
        print("ℹ️  Aucune donnée à transformer.")
        kwargs['ti'].xcom_push(key='clean_data', value=None)
        return
    df       = pd.read_json(raw)
    df_clean = transform(df)
    kwargs['ti'].xcom_push(key='clean_data', value=df_clean.to_json())

def run_load(**kwargs):
    import pandas as pd
    clean = kwargs['ti'].xcom_pull(key='clean_data')
    if clean is None:
        print("ℹ️  Aucune donnée à charger.")
        return
    df = pd.read_json(clean)
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], unit='ms')
    load(df)

with DAG(
    dag_id            = 'etl_amazon_pipeline',
    default_args      = default_args,
    schedule_interval = '@daily',
    catchup           = False,
    description       = 'Pipeline ETL Amazon Sales — dynamique (watermark)'
) as dag:
    t0 = PythonOperator(task_id='simulate', python_callable=run_simulate)
    t1 = PythonOperator(task_id='extract',   python_callable=run_extract)
    t2 = PythonOperator(task_id='transform', python_callable=run_transform)
    t3 = PythonOperator(task_id='load',      python_callable=run_load)

    t0 >> t1 >> t2 >> t3