import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('Matillion_Log', description='Matillion_Log_master job',
          schedule_interval="* 5,15,25,35,45,55 * * *",
          start_date=pendulum.datetime(2023, 11, 21, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)


m_Matillion_Log = EmptyOperator(task_id='Matillion_Log', job_name='Matillion_Log', dag=dag)


m_Matillion_Log << m_Start
m_End << m_Matillion_Log