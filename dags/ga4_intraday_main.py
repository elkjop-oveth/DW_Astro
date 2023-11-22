import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('GA4_intraday_Main', description='GA4_intraday_Main matillion job',
          schedule_interval="3 * * * *",
          start_date=pendulum.datetime(2023, 11, 22, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_GA4_intraday_Main = MatillionTriggerSyncOperator(task_id='GA4_intraday_Main', job_name='GA4_intraday_Main', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_GA4_intraday_Main << m_Start
m_End << m_GA4_intraday_Main

