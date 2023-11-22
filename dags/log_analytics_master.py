import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('LOG_ANALYTICS_Master', description='LOG_ANALYTICS_Master matillion job',
          schedule_interval="11 * * * *",
          start_date=pendulum.datetime(2023, 11, 21, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_LOG_ANALYTICS_Master = MatillionTriggerSyncOperator(task_id='LOG_ANALYTICS_Master', job_name='LOG_ANALYTICS_Master', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_LOG_ANALYTICS_Master << m_Start
m_End << m_LOG_ANALYTICS_Master
