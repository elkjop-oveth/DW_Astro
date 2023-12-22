import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('DW_QI_BLOCKED_STOCK', description='DW_QI_BLOCKED_STOCK matillion job',
          schedule_interval="0,10,20,30,40,50 * * * *",
          start_date=pendulum.datetime(2023, 12, 20, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_DW_QI_BLOCKED_STOCK = MatillionTriggerSyncOperator(task_id='DW_QI_BLOCKED_STOCK', job_name='DW_QI_BLOCKED_STOCK', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_DW_QI_BLOCKED_STOCK << m_Start
m_End << m_DW_QI_BLOCKED_STOCK


