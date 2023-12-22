import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('DW_HVR_SNAPSHOT_MASTER', description='DW_HVR_SNAPSHOT_MASTER matillion job',
          schedule_interval="10 * * * *",
          start_date=pendulum.datetime(2023, 12, 20, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)
m_DW_HVR_SNAPSHOT_MASTER = MatillionTriggerSyncOperator(task_id='DW_HVR_SNAPSHOT_MASTER', job_name='DW_HVR_SNAPSHOT_MASTER', group_name='DW', project_name='DW', environment_name='Production', trigger_rule='all_done', dag=dag)

m_DW_HVR_SNAPSHOT_MASTER << m_Start
m_End << m_DW_HVR_SNAPSHOT_MASTER
