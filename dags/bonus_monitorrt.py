import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('BONUS_MonitorRT', description='BONUS_MonitorRT matillion job',
          schedule_interval="49 9,10,11,13,14,15,16,17,18 * * *",
          start_date=pendulum.datetime(2023, 11, 22, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_BONUS_MonitorRT = MatillionTriggerSyncOperator(task_id='BONUS_MonitorRT', job_name='BONUS_MonitorRT', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_BONUS_MonitorRT << m_Start
m_End << m_BONUS_MonitorRT

