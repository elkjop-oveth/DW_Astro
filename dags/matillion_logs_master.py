import time
from datetime import datetime
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.timetables.trigger import CronTriggerTimetable
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('MATILLION_LOGS_Master', description='MATILLION_LOGS_Master matillion job',
          schedule=CronTriggerTimetable("5,15,25,35,45,55 * * * *", timezone="Europe/Oslo"),
          start_date=pendulum.datetime(2024, 1, 11, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_Matillions_Log = MatillionTriggerSyncOperator(task_id='MATILLION_LOGS_Master', job_name='MATILLION_LOGS_Master', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done',execution_timeout=timedelta(minutes=8), dag=dag)

m_Matillions_Log << m_Start
