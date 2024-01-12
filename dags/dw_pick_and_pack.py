import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('DW_PICK_AND_PACK', description='DW_PICK_AND_PACK matillion job',
          schedule=CronDataIntervalTimetable("15 06,08,11,13,15,17,19,21 * * *", timezone="Europe/Oslo"),
          start_date=pendulum.datetime(2024, 1, 10, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_DW_PICK_AND_PACK = MatillionTriggerSyncOperator(task_id='DW_PICK_AND_PACK', job_name='DW_PICK_AND_PACK', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_DW_PICK_AND_PACK << m_Start


