import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator
import os 
import certifi
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()

dag = DAG('DW_nightly_build', description='Matillion nightly execution',
          schedule_interval=None,
          start_date=pendulum.datetime(2023, 10, 31, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=1,
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_Test_Job = MatillionTriggerSyncOperator(task_id="mat_DW_DIMENSIONS", group_name="DW", project_name="DW_Test", job_name="DW_DIMENSIONS", environment_name="Test", dag=dag)
m_Test_Job << [m_Start]