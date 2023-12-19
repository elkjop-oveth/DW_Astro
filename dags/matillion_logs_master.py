import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('MATILLION_LOGS_Master', description='MATILLION_LOGS_Master matillion job',
          schedule_interval="5,15,25,35,45,55 * * * *",
          start_date=pendulum.datetime(2023, 12, 20, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_Matillions_Log = EmptyOperator(task_id='mat_matillion_log_master', dag=dag)
m_Matillions_Log = MatillionTriggerSyncOperator(task_id='MATILLION_LOGS_Master', job_name='MATILLION_LOGS_Master', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_Matillions_Log << m_Start
m_End << m_Matillions_Log