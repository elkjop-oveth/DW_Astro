import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

dag = DAG('complete_nightly_run', description='Complete nightly run',
          schedule_interval=None,
          start_date=pendulum.datetime(2023, 10, 31, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)


m_DW_C4S_SCORECARD_MASTER = EmptyOperator(task_id='mat_DW_C4S_SCORECARD_MASTER', matillion_job_name='DW_C4S_SCORECARD_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_TRANSPORT_DIM = EmptyOperator(task_id='mat_DW_TRANSPORT_DIM', matillion_job_name='DW_TRANSPORT_DIM', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_ODS_SAP_FULL_RANGE = EmptyOperator(task_id='mat_ODS_SAP_FULL_RANGE', matillion_job_name='ODS_SAP_FULL_RANGE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_IDENTITY_TAB_MASTER = EmptyOperator(task_id='mat_DW_IDENTITY_TAB_MASTER', matillion_job_name='DW_IDENTITY_TAB_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CAMPAIGN = EmptyOperator(task_id='mat_DW_CAMPAIGN', matillion_job_name='DW_CAMPAIGN', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_RFC = EmptyOperator(task_id='mat_DW_RFC', matillion_job_name='DW_RFC', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_MASTERSUPPLIER = EmptyOperator(task_id='mat_DW_MASTERSUPPLIER', matillion_job_name='DW_MASTERSUPPLIER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CUSTOMER_SATISFACTION = EmptyOperator(task_id='mat_DW_CUSTOMER_SATISFACTION', matillion_job_name='DW_CUSTOMER_SATISFACTION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DEPARTMENT_LFL = EmptyOperator(task_id='mat_DW_DEPARTMENT_LFL', matillion_job_name='DW_DEPARTMENT_LFL', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PIM = EmptyOperator(task_id='mat_DW_PIM', matillion_job_name='DW_PIM', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DIMENSIONS = EmptyOperator(task_id='mat_DW_DIMENSIONS', matillion_job_name='DW_DIMENSIONS', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CUSTOMER_RETURN = EmptyOperator(task_id='mat_DW_CUSTOMER_RETURN', matillion_job_name='DW_CUSTOMER_RETURN', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_EMPLOYEE = EmptyOperator(task_id='mat_DW_EMPLOYEE', matillion_job_name='DW_EMPLOYEE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ORGANIZATION = EmptyOperator(task_id='mat_DW_ORGANIZATION', matillion_job_name='DW_ORGANIZATION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_REPAIR_ORDER = EmptyOperator(task_id='mat_DW_REPAIR_ORDER', matillion_job_name='DW_REPAIR_ORDER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_FAULT_RATE = EmptyOperator(task_id='mat_DW_FAULT_RATE', matillion_job_name='DW_FAULT_RATE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_HON_AFTERSALE = EmptyOperator(task_id='mat_DW_HON_AFTERSALE', matillion_job_name='DW_HON_AFTERSALE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PROFITCENTER = EmptyOperator(task_id='mat_DW_PROFITCENTER', matillion_job_name='DW_PROFITCENTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CURRENCY = EmptyOperator(task_id='mat_DW_CURRENCY', matillion_job_name='DW_CURRENCY', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PERMISSION_TAB_MASTER = EmptyOperator(task_id='mat_DW_PERMISSION_TAB_MASTER', matillion_job_name='DW_PERMISSION_TAB_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_NGR_SO_MAIN = EmptyOperator(task_id='mat_DW_NGR_SO_MAIN', matillion_job_name='DW_NGR_SO_MAIN', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_RETAIL_COLLECT_AT_STORE = EmptyOperator(task_id='mat_DW_RETAIL_COLLECT_AT_STORE', matillion_job_name='DW_RETAIL_COLLECT_AT_STORE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DMF_TM = EmptyOperator(task_id='mat_DMF_TM', matillion_job_name='DMF_TM', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_STOCK = EmptyOperator(task_id='mat_DW_STOCK', matillion_job_name='DW_STOCK', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_WHS_SO_LINE_TAB = EmptyOperator(task_id='mat_DW_DMF_WHS_SO_LINE_TAB', matillion_job_name='DW_DMF_WHS_SO_LINE_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_Finance_Dimensions = EmptyOperator(task_id='mat_DW_Finance_Dimensions', matillion_job_name='DW_Finance_Dimensions', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_STORAGELOCATION = EmptyOperator(task_id='mat_DW_STORAGELOCATION', matillion_job_name='DW_STORAGELOCATION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_STOCK_RETAIL_CAMPAIGN_ASSORT = EmptyOperator(task_id='mat_DW_DMF_STOCK_RETAIL_CAMPAIGN_ASSORT', matillion_job_name='DW_DMF_STOCK_RETAIL_CAMPAIGN_ASSORT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_MATLOC = EmptyOperator(task_id='mat_DW_MATLOC', matillion_job_name='DW_MATLOC', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ARTICLE_ROLE = EmptyOperator(task_id='mat_DW_ARTICLE_ROLE', matillion_job_name='DW_ARTICLE_ROLE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_STOCK_Dimensions = EmptyOperator(task_id='mat_DW_STOCK_Dimensions', matillion_job_name='DW_STOCK_Dimensions', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_TRANSPORT = EmptyOperator(task_id='mat_DW_TRANSPORT', matillion_job_name='DW_TRANSPORT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_OVERSTOCK = EmptyOperator(task_id='mat_OVERSTOCK', matillion_job_name='OVERSTOCK', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ORDERTYPE = EmptyOperator(task_id='mat_DW_ORDERTYPE', matillion_job_name='DW_ORDERTYPE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_IA_SAP_HIST_TABLES = EmptyOperator(task_id='mat_IA_SAP_HIST_TABLES', matillion_job_name='IA_SAP_HIST_TABLES', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_WHS_DIMENSIONS = EmptyOperator(task_id='mat_DW_WHS_DIMENSIONS', matillion_job_name='DW_WHS_DIMENSIONS', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ABC_CLASSIFICATION = EmptyOperator(task_id='mat_DW_ABC_CLASSIFICATION', matillion_job_name='DW_ABC_CLASSIFICATION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_Stock_Master = EmptyOperator(task_id='mat_DW_Stock_Master', matillion_job_name='DW_Stock_Master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_WHS_MARGIN = EmptyOperator(task_id='mat_DW_WHS_MARGIN', matillion_job_name='DW_WHS_MARGIN', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_WHS_PO_LINE_TAB = EmptyOperator(task_id='mat_DW_DMF_WHS_PO_LINE_TAB', matillion_job_name='DW_DMF_WHS_PO_LINE_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_dmf_article_price_competitorinto_tab_master = EmptyOperator(task_id='mat_DW_dmf_article_price_competitorinto_tab_master', matillion_job_name='DW_dmf_article_price_competitorinto_tab_master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_STOCK_ATP7 = EmptyOperator(task_id='mat_DW_STOCK_ATP7', matillion_job_name='DW_STOCK_ATP7', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SALE = EmptyOperator(task_id='mat_DW_SALE', matillion_job_name='DW_SALE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SALE_BRIDGE_ORDER_TAB_MASTER = EmptyOperator(task_id='mat_DW_SALE_BRIDGE_ORDER_TAB_MASTER', matillion_job_name='DW_SALE_BRIDGE_ORDER_TAB_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SALE_RECURRING_SERVICES = EmptyOperator(task_id='mat_DW_SALE_RECURRING_SERVICES', matillion_job_name='DW_SALE_RECURRING_SERVICES', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SALE_ATTACHMENT = EmptyOperator(task_id='mat_DW_SALE_ATTACHMENT', matillion_job_name='DW_SALE_ATTACHMENT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_ZSD_STORESPLIT = EmptyOperator(task_id='mat_ZSD_STORESPLIT', matillion_job_name='ZSD_STORESPLIT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_MARKETING_PLAN_Master = EmptyOperator(task_id='mat_MARKETING_PLAN_Master', matillion_job_name='MARKETING_PLAN_Master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_Finance = EmptyOperator(task_id='mat_DW_Finance', matillion_job_name='DW_Finance', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_MIRAKL_Master = EmptyOperator(task_id='mat_MIRAKL_Master', matillion_job_name='MIRAKL_Master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CAMPAIGN_REFERENCES = EmptyOperator(task_id='mat_DW_CAMPAIGN_REFERENCES', matillion_job_name='DW_CAMPAIGN_REFERENCES', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_STOCK_WHS_GL = EmptyOperator(task_id='mat_DW_STOCK_WHS_GL', matillion_job_name='DW_STOCK_WHS_GL', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_FULL_RANGE = EmptyOperator(task_id='mat_DW_FULL_RANGE', matillion_job_name='DW_FULL_RANGE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PRICE_TAB = EmptyOperator(task_id='mat_DW_PRICE_TAB', matillion_job_name='DW_PRICE_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_FI_INVOICE_COCKPIT = EmptyOperator(task_id='mat_DW_FI_INVOICE_COCKPIT', matillion_job_name='DW_FI_INVOICE_COCKPIT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SUPPLIER_FUNDING_CONTRACT = EmptyOperator(task_id='mat_DW_SUPPLIER_FUNDING_CONTRACT', matillion_job_name='DW_SUPPLIER_FUNDING_CONTRACT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SALE_BLUEBERRY_BASKET_MASTER = EmptyOperator(task_id='mat_DW_SALE_BLUEBERRY_BASKET_MASTER', matillion_job_name='DW_SALE_BLUEBERRY_BASKET_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_IA_EMPLOYEE = EmptyOperator(task_id='mat_DW_IA_EMPLOYEE', matillion_job_name='DW_IA_EMPLOYEE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SEGMENTATION_MASTER = EmptyOperator(task_id='mat_DW_SEGMENTATION_MASTER', matillion_job_name='DW_SEGMENTATION_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_SUCCESSFACTORS_MASTER = EmptyOperator(task_id='mat_DW_SUCCESSFACTORS_MASTER', matillion_job_name='DW_SUCCESSFACTORS_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PAYMENT = EmptyOperator(task_id='mat_DW_PAYMENT', matillion_job_name='DW_PAYMENT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_COMMERCIAL_HUB_MASTER = EmptyOperator(task_id='mat_DW_COMMERCIAL_HUB_MASTER', matillion_job_name='DW_COMMERCIAL_HUB_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CC365_MASTER = EmptyOperator(task_id='mat_DW_CC365_MASTER', matillion_job_name='DW_CC365_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CLAIMS = EmptyOperator(task_id='mat_DW_CLAIMS', matillion_job_name='DW_CLAIMS', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_COMMERCIAL_RETUR = EmptyOperator(task_id='mat_DW_COMMERCIAL_RETUR', matillion_job_name='DW_COMMERCIAL_RETUR', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_COMPENSATION = EmptyOperator(task_id='mat_DW_COMPENSATION', matillion_job_name='DW_COMPENSATION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_AccountReceivables = EmptyOperator(task_id='mat_DW_AccountReceivables', matillion_job_name='DW_AccountReceivables', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PARTNER_B2B_TAB = EmptyOperator(task_id='mat_DW_PARTNER_B2B_TAB', matillion_job_name='DW_PARTNER_B2B_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_LAUNCHPAD_MESSAGE = EmptyOperator(task_id='mat_DW_DMF_LAUNCHPAD_MESSAGE', matillion_job_name='DW_DMF_LAUNCHPAD_MESSAGE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PRICE_CALCULATION = EmptyOperator(task_id='mat_DW_PRICE_CALCULATION', matillion_job_name='DW_PRICE_CALCULATION', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_WORK_TICKET = EmptyOperator(task_id='mat_DW_WORK_TICKET', matillion_job_name='DW_WORK_TICKET', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_TRADE_IN = EmptyOperator(task_id='mat_DW_TRADE_IN', matillion_job_name='DW_TRADE_IN', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_QUINYX_Master = EmptyOperator(task_id='mat_DW_QUINYX_Master', matillion_job_name='DW_QUINYX_Master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_PO_RETAIL = EmptyOperator(task_id='mat_DW_PO_RETAIL', matillion_job_name='DW_PO_RETAIL', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_SALE_B2B_TAB = EmptyOperator(task_id='mat_DW_DMF_SALE_B2B_TAB', matillion_job_name='DW_DMF_SALE_B2B_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_CUSTOMER = EmptyOperator(task_id='mat_DW_CUSTOMER', matillion_job_name='DW_CUSTOMER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DIGITAL_SIGNATURE_TAB = EmptyOperator(task_id='mat_DW_DIGITAL_SIGNATURE_TAB', matillion_job_name='DW_DIGITAL_SIGNATURE_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ENROLLMENT = EmptyOperator(task_id='mat_DW_ENROLLMENT', matillion_job_name='DW_ENROLLMENT', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_MARKETING_SALES = EmptyOperator(task_id='mat_DW_MARKETING_SALES', matillion_job_name='DW_MARKETING_SALES', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ODS_ZFRE_SLS_HIST_IF_TAB = EmptyOperator(task_id='mat_DW_ODS_ZFRE_SLS_HIST_IF_TAB', matillion_job_name='DW_ODS_ZFRE_SLS_HIST_IF_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_LOGISTIC_Master = EmptyOperator(task_id='mat_DW_LOGISTIC_Master', matillion_job_name='DW_LOGISTIC_Master', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_HR_PEOPLEPORTAL_MASTER = EmptyOperator(task_id='mat_DW_HR_PEOPLEPORTAL_MASTER', matillion_job_name='DW_HR_PEOPLEPORTAL_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_EXCEPTION_FRE = EmptyOperator(task_id='mat_DW_EXCEPTION_FRE', matillion_job_name='DW_EXCEPTION_FRE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_ACADEMY_MASTER = EmptyOperator(task_id='mat_DW_ACADEMY_MASTER', matillion_job_name='DW_ACADEMY_MASTER', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_INSURANCE = EmptyOperator(task_id='mat_DW_INSURANCE', matillion_job_name='DW_INSURANCE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_EPOQ_SALES = EmptyOperator(task_id='mat_DW_EPOQ_SALES', matillion_job_name='DW_EPOQ_SALES', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_SALE_RETAIL_CAS_TAB = EmptyOperator(task_id='mat_DW_DMF_SALE_RETAIL_CAS_TAB', matillion_job_name='DW_DMF_SALE_RETAIL_CAS_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_APPOINTMENTS = EmptyOperator(task_id='mat_DW_APPOINTMENTS', matillion_job_name='DW_APPOINTMENTS', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_LIFECYCLE = EmptyOperator(task_id='mat_DW_DMF_LIFECYCLE', matillion_job_name='DW_DMF_LIFECYCLE', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_DMF_SALE_RETURN_ORDER_TAB = EmptyOperator(task_id='mat_DW_DMF_SALE_RETURN_ORDER_TAB', matillion_job_name='DW_DMF_SALE_RETURN_ORDER_TAB', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)
m_DW_RFC_CURRENT_WHS = EmptyOperator(task_id='mat_DW_RFC_CURRENT_WHS', matillion_job_name='DW_RFC_CURRENT_WHS', matillion_group_name = 'DW', matillion_project_name = 'DW', matillion_environment_Name='nightly_build_test', dag=dag)


m_DW_C4S_SCORECARD_MASTER << [m_DW_REPAIR_ORDER,m_DW_RETAIL_COLLECT_AT_STORE]
m_DW_TRANSPORT_DIM << [m_DW_ORGANIZATION]
m_ODS_SAP_FULL_RANGE << [m_DW_PIM]
m_DW_IDENTITY_TAB_MASTER << [m_Start]
m_DW_CAMPAIGN << [m_DW_PIM]
m_DW_RFC << [m_DW_WHS_MARGIN,m_DW_CUSTOMER_RETURN]
m_DW_MASTERSUPPLIER << [m_Start]
m_DW_CUSTOMER_SATISFACTION << [m_DW_REPAIR_ORDER,m_DW_CC365_MASTER]
m_DW_DEPARTMENT_LFL << [m_DW_ORGANIZATION]
m_DW_PIM << [m_DW_MASTERSUPPLIER,m_DW_EMPLOYEE]
m_DW_DIMENSIONS << [m_DW_IDENTITY_TAB_MASTER]
m_DW_CUSTOMER_RETURN << [m_DW_REPAIR_ORDER]
m_DW_EMPLOYEE << [m_DW_ORGANIZATION]
m_DW_ORGANIZATION << [m_DW_PROFITCENTER]
m_DW_REPAIR_ORDER << [m_DW_HON_AFTERSALE]
m_DW_FAULT_RATE << [m_DW_RFC]
m_DW_HON_AFTERSALE << [m_DMF_TM]
m_DW_PROFITCENTER << [m_Start]
m_DW_CURRENCY << [m_Start]
m_DW_PERMISSION_TAB_MASTER << [m_DW_ORGANIZATION]
m_DW_NGR_SO_MAIN << [m_DW_CAMPAIGN,m_DW_DEPARTMENT_LFL,m_DW_PERMISSION_TAB_MASTER,m_DW_DIMENSIONS,m_DW_CURRENCY,m_ODS_SAP_FULL_RANGE]
m_DW_RETAIL_COLLECT_AT_STORE << [m_DW_NGR_SO_MAIN]
m_DMF_TM << [m_DW_TRANSPORT_DIM,m_DW_NGR_SO_MAIN]
m_DW_STOCK << [m_DW_DMF_WHS_PO_LINE_TAB]
m_DW_DMF_WHS_SO_LINE_TAB << [m_DW_STORAGELOCATION,m_DW_CAMPAIGN,m_DW_CURRENCY]
m_DW_Finance_Dimensions << [m_DW_PROFITCENTER]
m_DW_STORAGELOCATION << [m_DW_STOCK_Dimensions]
m_DW_DMF_STOCK_RETAIL_CAMPAIGN_ASSORT << [m_DW_MATLOC]
m_DW_MATLOC << [m_DW_DMF_WHS_PO_LINE_TAB,m_DW_ARTICLE_ROLE]
m_DW_ARTICLE_ROLE << [m_ODS_SAP_FULL_RANGE]
m_DW_STOCK_Dimensions << [m_DW_WHS_DIMENSIONS]
m_DW_TRANSPORT << [m_DMF_TM]
m_OVERSTOCK << [m_DW_DMF_LIFECYCLE,m_DW_DMF_WHS_PO_LINE_TAB]
m_DW_ORDERTYPE << [m_Start]
m_IA_SAP_HIST_TABLES << [m_Start]
m_DW_WHS_DIMENSIONS << [m_Start]
m_DW_ABC_CLASSIFICATION << [m_DW_WHS_MARGIN]
m_DW_Stock_Master << [m_DW_STOCK_Dimensions,m_DW_CURRENCY,m_IA_SAP_HIST_TABLES,m_ODS_SAP_FULL_RANGE]
m_DW_WHS_MARGIN << [m_DW_ORDERTYPE,m_DW_NGR_SO_MAIN,m_DW_DMF_WHS_SO_LINE_TAB,m_DW_Finance_Dimensions]
m_DW_DMF_WHS_PO_LINE_TAB << [m_DW_STOCK_WHS_GL,m_DW_ABC_CLASSIFICATION,m_DW_Stock_Master]
m_DW_dmf_article_price_competitorinto_tab_master << [m_DW_PRICE_TAB,m_DW_DMF_WHS_PO_LINE_TAB]
m_DW_STOCK_ATP7 << [m_DW_DMF_LIFECYCLE,m_DW_ABC_CLASSIFICATION]
m_DW_SALE << [m_DW_PAYMENT]
m_DW_SALE_BRIDGE_ORDER_TAB_MASTER << [m_DW_NGR_SO_MAIN]
m_DW_SALE_RECURRING_SERVICES << [m_DW_NGR_SO_MAIN]
m_DW_SALE_ATTACHMENT << [m_DW_NGR_SO_MAIN]
m_ZSD_STORESPLIT << [m_DW_NGR_SO_MAIN]
m_MARKETING_PLAN_Master << [m_DW_Finance,m_DW_NGR_SO_MAIN]
m_DW_Finance << [m_DW_Finance_Dimensions,m_DW_DEPARTMENT_LFL,m_DW_PIM,m_DW_CURRENCY]
m_MIRAKL_Master << [m_DW_NGR_SO_MAIN]
m_DW_CAMPAIGN_REFERENCES << [m_DW_CAMPAIGN,m_DW_WHS_DIMENSIONS,m_DW_CURRENCY]
m_DW_STOCK_WHS_GL << [m_DW_STOCK_Dimensions,m_DW_PIM,m_DW_CURRENCY]
m_DW_FULL_RANGE << [m_DW_PRICE_TAB,m_ODS_SAP_FULL_RANGE]
m_DW_PRICE_TAB << [m_DW_PIM,m_DW_CURRENCY]
m_DW_FI_INVOICE_COCKPIT << [m_DW_Finance_Dimensions,m_DW_PO_RETAIL]
m_DW_SUPPLIER_FUNDING_CONTRACT << [m_DW_NGR_SO_MAIN]
m_DW_SALE_BLUEBERRY_BASKET_MASTER << [m_DW_PIM,m_DW_IA_EMPLOYEE,m_DW_CURRENCY]
m_DW_IA_EMPLOYEE << [m_DW_ORGANIZATION]
m_DW_SEGMENTATION_MASTER << [m_DW_IDENTITY_TAB_MASTER,m_DW_PERMISSION_TAB_MASTER,m_DW_EMPLOYEE]
m_DW_SUCCESSFACTORS_MASTER << [m_DW_EMPLOYEE]
m_DW_PAYMENT << [m_DW_NGR_SO_MAIN]
m_DW_COMMERCIAL_HUB_MASTER << [m_DW_WHS_MARGIN,m_DW_STOCK_WHS_GL,m_DW_Stock_Master]
m_DW_CC365_MASTER << [m_DW_NGR_SO_MAIN]
m_DW_CLAIMS << [m_DW_Stock_Master]
m_DW_COMMERCIAL_RETUR << [m_DW_Stock_Master]
m_DW_COMPENSATION << [m_DW_NGR_SO_MAIN]
m_DW_AccountReceivables << [m_DW_Finance_Dimensions]
m_DW_PARTNER_B2B_TAB << [m_DW_DIMENSIONS]
m_DW_DMF_LAUNCHPAD_MESSAGE << [m_DW_EMPLOYEE]
m_DW_PRICE_CALCULATION << [m_DW_PIM,m_DW_CURRENCY]
m_DW_WORK_TICKET << [m_DW_PIM]
m_DW_TRADE_IN << [m_DW_PIM,m_DW_CURRENCY]
m_DW_QUINYX_Master << [m_DW_NGR_SO_MAIN]
m_DW_PO_RETAIL << [m_DW_ORDERTYPE,m_DW_PIM,m_DW_WHS_DIMENSIONS,m_DW_CURRENCY]
m_DW_DMF_SALE_B2B_TAB << [m_DW_PARTNER_B2B_TAB,m_DW_NGR_SO_MAIN]
m_DW_CUSTOMER << [m_DW_NGR_SO_MAIN]
m_DW_DIGITAL_SIGNATURE_TAB << [m_DW_NGR_SO_MAIN]
m_DW_ENROLLMENT << [m_DW_DMF_SALE_B2B_TAB]
m_DW_MARKETING_SALES << [m_DW_NGR_SO_MAIN]
m_DW_ODS_ZFRE_SLS_HIST_IF_TAB << [m_DW_NGR_SO_MAIN]
m_DW_LOGISTIC_Master << [m_DW_NGR_SO_MAIN]
m_DW_HR_PEOPLEPORTAL_MASTER << [m_DW_EMPLOYEE]
m_DW_EXCEPTION_FRE << [m_DW_PIM]
m_DW_ACADEMY_MASTER << [m_DW_EMPLOYEE]
m_DW_INSURANCE << [m_DW_MASTERSUPPLIER,m_DW_ORGANIZATION,m_DW_CURRENCY]
m_DW_EPOQ_SALES << [m_DW_NGR_SO_MAIN]
m_DW_DMF_SALE_RETAIL_CAS_TAB << [m_DW_NGR_SO_MAIN]
m_DW_APPOINTMENTS << [m_DW_MASTERSUPPLIER,m_DW_ORGANIZATION]
m_DW_DMF_LIFECYCLE << [m_DW_PIM]
m_DW_DMF_SALE_RETURN_ORDER_TAB << [m_DW_ORDERTYPE,m_DW_NGR_SO_MAIN]
m_DW_RFC_CURRENT_WHS << [m_DW_RFC]

m_End << [m_DW_C4S_SCORECARD_MASTER,m_DW_CUSTOMER_SATISFACTION,m_DW_FAULT_RATE,m_DW_STOCK,m_DW_DMF_STOCK_RETAIL_CAMPAIGN_ASSORT,m_DW_TRANSPORT,m_OVERSTOCK,m_DW_dmf_article_price_competitorinto_tab_master,m_DW_STOCK_ATP7,m_DW_SALE,m_DW_SALE_BRIDGE_ORDER_TAB_MASTER,m_DW_SALE_RECURRING_SERVICES,m_DW_SALE_ATTACHMENT,m_ZSD_STORESPLIT,m_MARKETING_PLAN_Master,m_MIRAKL_Master,m_DW_CAMPAIGN_REFERENCES,m_DW_FULL_RANGE,m_DW_FI_INVOICE_COCKPIT,m_DW_SUPPLIER_FUNDING_CONTRACT,m_DW_SALE_BLUEBERRY_BASKET_MASTER,m_DW_SEGMENTATION_MASTER,m_DW_SUCCESSFACTORS_MASTER,m_DW_COMMERCIAL_HUB_MASTER,m_DW_CLAIMS,m_DW_COMMERCIAL_RETUR,m_DW_COMPENSATION,m_DW_AccountReceivables,m_DW_DMF_LAUNCHPAD_MESSAGE,m_DW_PRICE_CALCULATION,m_DW_WORK_TICKET,m_DW_TRADE_IN,m_DW_QUINYX_Master,m_DW_CUSTOMER,m_DW_DIGITAL_SIGNATURE_TAB,m_DW_ENROLLMENT,m_DW_MARKETING_SALES,m_DW_ODS_ZFRE_SLS_HIST_IF_TAB,m_DW_LOGISTIC_Master,m_DW_HR_PEOPLEPORTAL_MASTER,m_DW_EXCEPTION_FRE,m_DW_ACADEMY_MASTER,m_DW_INSURANCE,m_DW_EPOQ_SALES,m_DW_DMF_SALE_RETAIL_CAS_TAB,m_DW_APPOINTMENTS,m_DW_DMF_SALE_RETURN_ORDER_TAB,m_DW_RFC_CURRENT_WHS]