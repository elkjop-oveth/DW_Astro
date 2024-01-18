import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.timetables.interval import CronDataIntervalTimetable
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator
dag = DAG('DW_CUSTOMER_TAB', description='DW_CUSTOMER_TAB matillion job',
          schedule=CronDataIntervalTimetable("2 * * * *", timezone="Europe/Oslo"),
          schedule_interval="",
          start_date=pendulum.datetime(2023, 12, 20, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_DW_CUSTOMER_TAB = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER_TAB', job_name='DW_CUSTOMER_TAB', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_DW_CUSTOMER_TAB << m_Start
