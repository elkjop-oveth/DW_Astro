import time
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from include.matillion.operators.MatillionTriggerSyncOperator import MatillionTriggerSyncOperator

dag = DAG('dagtest', description='DW nightly batch execution',
          schedule_interval=None,
          start_date=pendulum.datetime(2022, 10, 25, tz="Europe/Oslo"),
          max_active_runs=1,
          concurrency=8,
          tags=["Matillion"],
          catchup=False
          )

m_Start = EmptyOperator(task_id='Start', dag=dag)
m_End = EmptyOperator(task_id='End', trigger_rule='all_done', dag=dag)


m_DMF_TM = MatillionTriggerSyncOperator(task_id='DMF_TM', job_name='DMF_TM', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ABC_CLASSIFICATION = MatillionTriggerSyncOperator(task_id='DW_ABC_CLASSIFICATION', job_name='DW_ABC_CLASSIFICATION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ACADEMY_MASTER = MatillionTriggerSyncOperator(task_id='DW_ACADEMY_MASTER', job_name='DW_ACADEMY_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_APPOINTMENTS = MatillionTriggerSyncOperator(task_id='DW_APPOINTMENTS', job_name='DW_APPOINTMENTS', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ARTICLE_ROLE = MatillionTriggerSyncOperator(task_id='DW_ARTICLE_ROLE', job_name='DW_ARTICLE_ROLE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ASSORTMENT_TAB = MatillionTriggerSyncOperator(task_id='DW_ASSORTMENT_TAB', job_name='DW_ASSORTMENT_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_AccountReceivables = MatillionTriggerSyncOperator(task_id='DW_AccountReceivables', job_name='DW_AccountReceivables', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_BAZAARVOICE_Master = MatillionTriggerSyncOperator(task_id='DW_BAZAARVOICE_Master', job_name='DW_BAZAARVOICE_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_BUDGET_Master = MatillionTriggerSyncOperator(task_id='DW_BUDGET_Master', job_name='DW_BUDGET_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_C4S_SCORECARD_MASTER = MatillionTriggerSyncOperator(task_id='DW_C4S_SCORECARD_MASTER', job_name='DW_C4S_SCORECARD_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CAMPAIGN = MatillionTriggerSyncOperator(task_id='DW_CAMPAIGN', job_name='DW_CAMPAIGN', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CAMPAIGN_REFERENCES = MatillionTriggerSyncOperator(task_id='DW_CAMPAIGN_REFERENCES', job_name='DW_CAMPAIGN_REFERENCES', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CC365_MASTER = MatillionTriggerSyncOperator(task_id='DW_CC365_MASTER', job_name='DW_CC365_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CLAIMS = MatillionTriggerSyncOperator(task_id='DW_CLAIMS', job_name='DW_CLAIMS', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_COMMERCIAL_HUB_MASTER = MatillionTriggerSyncOperator(task_id='DW_COMMERCIAL_HUB_MASTER', job_name='DW_COMMERCIAL_HUB_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_COMMERCIAL_RETUR = MatillionTriggerSyncOperator(task_id='DW_COMMERCIAL_RETUR', job_name='DW_COMMERCIAL_RETUR', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_COMPENSATION = MatillionTriggerSyncOperator(task_id='DW_COMPENSATION', job_name='DW_COMPENSATION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CURRENCY = MatillionTriggerSyncOperator(task_id='DW_CURRENCY', job_name='DW_CURRENCY', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CUSTOMER = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER', job_name='DW_CUSTOMER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CUSTOMER_EXCLUSION = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER_EXCLUSION', job_name='DW_CUSTOMER_EXCLUSION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CUSTOMER_RECRUITMENT = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER_RECRUITMENT', job_name='DW_CUSTOMER_RECRUITMENT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CUSTOMER_RETURN = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER_RETURN', job_name='DW_CUSTOMER_RETURN', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_CUSTOMER_SATISFACTION = MatillionTriggerSyncOperator(task_id='DW_CUSTOMER_SATISFACTION', job_name='DW_CUSTOMER_SATISFACTION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DEPARTMENT_LFL = MatillionTriggerSyncOperator(task_id='DW_DEPARTMENT_LFL', job_name='DW_DEPARTMENT_LFL', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DIGITAL_SIGNATURE_TAB = MatillionTriggerSyncOperator(task_id='DW_DIGITAL_SIGNATURE_TAB', job_name='DW_DIGITAL_SIGNATURE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DIMENSIONS = MatillionTriggerSyncOperator(task_id='DW_DIMENSIONS', job_name='DW_DIMENSIONS', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_LAUNCHPAD_MESSAGE = MatillionTriggerSyncOperator(task_id='DW_DMF_LAUNCHPAD_MESSAGE', job_name='DW_DMF_LAUNCHPAD_MESSAGE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_LIFECYCLE = MatillionTriggerSyncOperator(task_id='DW_DMF_LIFECYCLE', job_name='DW_DMF_LIFECYCLE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SALE_B2B_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_SALE_B2B_TAB', job_name='DW_DMF_SALE_B2B_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SALE_ORDER_LINE_AGG_temporary = MatillionTriggerSyncOperator(task_id='DW_DMF_SALE_ORDER_LINE_AGG_temporary', job_name='DW_DMF_SALE_ORDER_LINE_AGG_temporary', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SALE_OTIF_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_SALE_OTIF_TAB', job_name='DW_DMF_SALE_OTIF_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SALE_RETAIL_CAS_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_SALE_RETAIL_CAS_TAB', job_name='DW_DMF_SALE_RETAIL_CAS_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SALE_RETURN_ORDER_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_SALE_RETURN_ORDER_TAB', job_name='DW_DMF_SALE_RETURN_ORDER_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_STORE_OPENING_HOURS_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_STORE_OPENING_HOURS_TAB', job_name='DW_DMF_STORE_OPENING_HOURS_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_SUPPLIER_FUNDING_METRICS_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_SUPPLIER_FUNDING_METRICS_TAB', job_name='DW_DMF_SUPPLIER_FUNDING_METRICS_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_WHS_PO_CHANGE_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_WHS_PO_CHANGE_TAB', job_name='DW_DMF_WHS_PO_CHANGE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_WHS_PO_LINE_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_WHS_PO_LINE_TAB', job_name='DW_DMF_WHS_PO_LINE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DMF_WHS_SO_LINE_TAB = MatillionTriggerSyncOperator(task_id='DW_DMF_WHS_SO_LINE_TAB', job_name='DW_DMF_WHS_SO_LINE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_DeleteMe_Master = MatillionTriggerSyncOperator(task_id='DW_DeleteMe_Master', job_name='DW_DeleteMe_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ECOM_MOSTPOPULAR_Master = MatillionTriggerSyncOperator(task_id='DW_ECOM_MOSTPOPULAR_Master', job_name='DW_ECOM_MOSTPOPULAR_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_EMPLOYEE = MatillionTriggerSyncOperator(task_id='DW_EMPLOYEE', job_name='DW_EMPLOYEE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_EPOQ_SALES = MatillionTriggerSyncOperator(task_id='DW_EPOQ_SALES', job_name='DW_EPOQ_SALES', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_EXCEPTION_FRE = MatillionTriggerSyncOperator(task_id='DW_EXCEPTION_FRE', job_name='DW_EXCEPTION_FRE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_FAULT_RATE = MatillionTriggerSyncOperator(task_id='DW_FAULT_RATE', job_name='DW_FAULT_RATE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_FI_CHEMICAL_TAX = MatillionTriggerSyncOperator(task_id='DW_FI_CHEMICAL_TAX', job_name='DW_FI_CHEMICAL_TAX', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_FI_INVOICE_COCKPIT = MatillionTriggerSyncOperator(task_id='DW_FI_INVOICE_COCKPIT', job_name='DW_FI_INVOICE_COCKPIT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_FORECAST_FRE_HIST = MatillionTriggerSyncOperator(task_id='DW_FORECAST_FRE_HIST', job_name='DW_FORECAST_FRE_HIST', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_FULL_RANGE = MatillionTriggerSyncOperator(task_id='DW_FULL_RANGE', job_name='DW_FULL_RANGE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_Finance = MatillionTriggerSyncOperator(task_id='DW_Finance', job_name='DW_Finance', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_Finance_Dimensions = MatillionTriggerSyncOperator(task_id='DW_Finance_Dimensions', job_name='DW_Finance_Dimensions', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_HON_AFTERSALE = MatillionTriggerSyncOperator(task_id='DW_HON_AFTERSALE', job_name='DW_HON_AFTERSALE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_HR_PEOPLEPORTAL_MASTER = MatillionTriggerSyncOperator(task_id='DW_HR_PEOPLEPORTAL_MASTER', job_name='DW_HR_PEOPLEPORTAL_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_HR_QUINYX_Master = MatillionTriggerSyncOperator(task_id='DW_HR_QUINYX_Master', job_name='DW_HR_QUINYX_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_IA_EMPLOYEE = MatillionTriggerSyncOperator(task_id='DW_IA_EMPLOYEE', job_name='DW_IA_EMPLOYEE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_IDENTITY_TAB_MASTER = MatillionTriggerSyncOperator(task_id='DW_IDENTITY_TAB_MASTER', job_name='DW_IDENTITY_TAB_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_INSURANCE = MatillionTriggerSyncOperator(task_id='DW_INSURANCE', job_name='DW_INSURANCE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_INTAKE = MatillionTriggerSyncOperator(task_id='DW_INTAKE', job_name='DW_INTAKE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_KPI_REPORTING = MatillionTriggerSyncOperator(task_id='DW_KPI_REPORTING', job_name='DW_KPI_REPORTING', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_LOGISTIC_Master = MatillionTriggerSyncOperator(task_id='DW_LOGISTIC_Master', job_name='DW_LOGISTIC_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_MARKETING_SALES = MatillionTriggerSyncOperator(task_id='DW_MARKETING_SALES', job_name='DW_MARKETING_SALES', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_MASTERSUPPLIER = MatillionTriggerSyncOperator(task_id='DW_MASTERSUPPLIER', job_name='DW_MASTERSUPPLIER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_MATLOC = MatillionTriggerSyncOperator(task_id='DW_MATLOC', job_name='DW_MATLOC', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_NGR_SO_MAIN = MatillionTriggerSyncOperator(task_id='DW_NGR_SO_MAIN', job_name='DW_NGR_SO_MAIN', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ODS_ZFRE_SLS_HIST_IF_TAB = MatillionTriggerSyncOperator(task_id='DW_ODS_ZFRE_SLS_HIST_IF_TAB', job_name='DW_ODS_ZFRE_SLS_HIST_IF_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ORDERTYPE = MatillionTriggerSyncOperator(task_id='DW_ORDERTYPE', job_name='DW_ORDERTYPE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_ORGANIZATION = MatillionTriggerSyncOperator(task_id='DW_ORGANIZATION', job_name='DW_ORGANIZATION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_OUTSTANDING_AMOUNT_RFC = MatillionTriggerSyncOperator(task_id='DW_OUTSTANDING_AMOUNT_RFC', job_name='DW_OUTSTANDING_AMOUNT_RFC', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PARTNER_B2B_TAB = MatillionTriggerSyncOperator(task_id='DW_PARTNER_B2B_TAB', job_name='DW_PARTNER_B2B_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PAYMENT = MatillionTriggerSyncOperator(task_id='DW_PAYMENT', job_name='DW_PAYMENT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PERMISSION_TAB_MASTER = MatillionTriggerSyncOperator(task_id='DW_PERMISSION_TAB_MASTER', job_name='DW_PERMISSION_TAB_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PIM = MatillionTriggerSyncOperator(task_id='DW_PIM', job_name='DW_PIM', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PIM_ARTICLECONTENT = MatillionTriggerSyncOperator(task_id='DW_PIM_ARTICLECONTENT', job_name='DW_PIM_ARTICLECONTENT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_POS_ORDER_UPDATE_TAB = MatillionTriggerSyncOperator(task_id='DW_POS_ORDER_UPDATE_TAB', job_name='DW_POS_ORDER_UPDATE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PO_RETAIL = MatillionTriggerSyncOperator(task_id='DW_PO_RETAIL', job_name='DW_PO_RETAIL', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PRICE_CALCULATION = MatillionTriggerSyncOperator(task_id='DW_PRICE_CALCULATION', job_name='DW_PRICE_CALCULATION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PRICE_TAB = MatillionTriggerSyncOperator(task_id='DW_PRICE_TAB', job_name='DW_PRICE_TAB', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PROFITCENTER = MatillionTriggerSyncOperator(task_id='DW_PROFITCENTER', job_name='DW_PROFITCENTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_PnP = MatillionTriggerSyncOperator(task_id='DW_PnP', job_name='DW_PnP', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_QUINYX_Master = MatillionTriggerSyncOperator(task_id='DW_QUINYX_Master', job_name='DW_QUINYX_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_REPAIR_ORDER = MatillionTriggerSyncOperator(task_id='DW_REPAIR_ORDER', job_name='DW_REPAIR_ORDER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_RETAIL_COLLECT_AT_STORE = MatillionTriggerSyncOperator(task_id='DW_RETAIL_COLLECT_AT_STORE', job_name='DW_RETAIL_COLLECT_AT_STORE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_RFC = MatillionTriggerSyncOperator(task_id='DW_RFC', job_name='DW_RFC', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_RFC_CURRENT_WHS = MatillionTriggerSyncOperator(task_id='DW_RFC_CURRENT_WHS', job_name='DW_RFC_CURRENT_WHS', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE = MatillionTriggerSyncOperator(task_id='DW_SALE', job_name='DW_SALE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_ATTACHMENT = MatillionTriggerSyncOperator(task_id='DW_SALE_ATTACHMENT', job_name='DW_SALE_ATTACHMENT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_BLUEBERRY_BASKET_MASTER = MatillionTriggerSyncOperator(task_id='DW_SALE_BLUEBERRY_BASKET_MASTER', job_name='DW_SALE_BLUEBERRY_BASKET_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_BRIDGE_ORDER_TAB_MASTER = MatillionTriggerSyncOperator(task_id='DW_SALE_BRIDGE_ORDER_TAB_MASTER', job_name='DW_SALE_BRIDGE_ORDER_TAB_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_CANCEL_ORDER = MatillionTriggerSyncOperator(task_id='DW_SALE_CANCEL_ORDER', job_name='DW_SALE_CANCEL_ORDER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_COMMERCIAL_SERVICES_HIT_RATE = MatillionTriggerSyncOperator(task_id='DW_SALE_COMMERCIAL_SERVICES_HIT_RATE', job_name='DW_SALE_COMMERCIAL_SERVICES_HIT_RATE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_DOCUMENT_MASTER = MatillionTriggerSyncOperator(task_id='DW_SALE_DOCUMENT_MASTER', job_name='DW_SALE_DOCUMENT_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_RECURRING_SERVICES = MatillionTriggerSyncOperator(task_id='DW_SALE_RECURRING_SERVICES', job_name='DW_SALE_RECURRING_SERVICES', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SALE_SUBSCRIPTION_SELECTOR = MatillionTriggerSyncOperator(task_id='DW_SALE_SUBSCRIPTION_SELECTOR', job_name='DW_SALE_SUBSCRIPTION_SELECTOR', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SEGMENTATION_MASTER = MatillionTriggerSyncOperator(task_id='DW_SEGMENTATION_MASTER', job_name='DW_SEGMENTATION_MASTER', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SOLD_NOT_DELIVERED = MatillionTriggerSyncOperator(task_id='DW_SOLD_NOT_DELIVERED', job_name='DW_SOLD_NOT_DELIVERED', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_STOCK = MatillionTriggerSyncOperator(task_id='DW_STOCK', job_name='DW_STOCK', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_STOCK_ATP7 = MatillionTriggerSyncOperator(task_id='DW_STOCK_ATP7', job_name='DW_STOCK_ATP7', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_STOCK_Dimensions = MatillionTriggerSyncOperator(task_id='DW_STOCK_Dimensions', job_name='DW_STOCK_Dimensions', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_STOCK_WHS_GL = MatillionTriggerSyncOperator(task_id='DW_STOCK_WHS_GL', job_name='DW_STOCK_WHS_GL', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_STORAGELOCATION = MatillionTriggerSyncOperator(task_id='DW_STORAGELOCATION', job_name='DW_STORAGELOCATION', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SUPPLIER_FUNDING_CONTRACT = MatillionTriggerSyncOperator(task_id='DW_SUPPLIER_FUNDING_CONTRACT', job_name='DW_SUPPLIER_FUNDING_CONTRACT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_SURVEY = MatillionTriggerSyncOperator(task_id='DW_SURVEY', job_name='DW_SURVEY', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_Stock_Master = MatillionTriggerSyncOperator(task_id='DW_Stock_Master', job_name='DW_Stock_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TIME = MatillionTriggerSyncOperator(task_id='DW_TIME', job_name='DW_TIME', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TM_NEW = MatillionTriggerSyncOperator(task_id='DW_TM_NEW', job_name='DW_TM_NEW', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TRADE_IN = MatillionTriggerSyncOperator(task_id='DW_TRADE_IN', job_name='DW_TRADE_IN', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TRANSPORT = MatillionTriggerSyncOperator(task_id='DW_TRANSPORT', job_name='DW_TRANSPORT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TRANSPORT_DIM = MatillionTriggerSyncOperator(task_id='DW_TRANSPORT_DIM', job_name='DW_TRANSPORT_DIM', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_TRANSPORT_INSURANCE = MatillionTriggerSyncOperator(task_id='DW_TRANSPORT_INSURANCE', job_name='DW_TRANSPORT_INSURANCE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_UNBOOKED_RETURNSTORE = MatillionTriggerSyncOperator(task_id='DW_UNBOOKED_RETURNSTORE', job_name='DW_UNBOOKED_RETURNSTORE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_WHS_DIMENSIONS = MatillionTriggerSyncOperator(task_id='DW_WHS_DIMENSIONS', job_name='DW_WHS_DIMENSIONS', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_WHS_MARGIN = MatillionTriggerSyncOperator(task_id='DW_WHS_MARGIN', job_name='DW_WHS_MARGIN', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_WORK_TICKET = MatillionTriggerSyncOperator(task_id='DW_WORK_TICKET', job_name='DW_WORK_TICKET', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_DW_dmf_article_price_competitorinto_tab_master = MatillionTriggerSyncOperator(task_id='DW_dmf_article_price_competitorinto_tab_master', job_name='DW_dmf_article_price_competitorinto_tab_master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_EMPLOYEE_DISCOUNT_master = MatillionTriggerSyncOperator(task_id='EMPLOYEE_DISCOUNT_master', job_name='EMPLOYEE_DISCOUNT_master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_IA_SAP_HIST_TABLES = MatillionTriggerSyncOperator(task_id='IA_SAP_HIST_TABLES', job_name='IA_SAP_HIST_TABLES', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_MARKETING_PLAN_Master = MatillionTriggerSyncOperator(task_id='MARKETING_PLAN_Master', job_name='MARKETING_PLAN_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_MIRAKL_Master = MatillionTriggerSyncOperator(task_id='MIRAKL_Master', job_name='MIRAKL_Master', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_ODS_SAP_FULL_RANGE = MatillionTriggerSyncOperator(task_id='ODS_SAP_FULL_RANGE', job_name='ODS_SAP_FULL_RANGE', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_OVERSTOCK = MatillionTriggerSyncOperator(task_id='OVERSTOCK', job_name='OVERSTOCK', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)
m_ZSD_STORESPLIT = MatillionTriggerSyncOperator(task_id='ZSD_STORESPLIT', job_name='ZSD_STORESPLIT', group_name='DW', project_name='DW_Test', environment_name='Test', trigger_rule='all_done', dag=dag)


m_DMF_TM << m_DW_NGR_SO_MAIN
m_DMF_TM << m_DW_TRANSPORT_DIM
m_DW_ABC_CLASSIFICATION << m_DW_WHS_MARGIN
m_DW_ACADEMY_MASTER << m_DW_EMPLOYEE
m_DW_APPOINTMENTS << m_DW_MASTERSUPPLIER
m_DW_APPOINTMENTS << m_DW_ORGANIZATION
m_DW_ARTICLE_ROLE << m_ODS_SAP_FULL_RANGE
m_DW_ASSORTMENT_TAB << m_IA_SAP_HIST_TABLES
m_DW_ASSORTMENT_TAB << m_DW_STOCK_ATP7
m_DW_AccountReceivables << m_DW_TIME
m_DW_AccountReceivables << m_DW_Finance_Dimensions
m_DW_BAZAARVOICE_Master << m_DW_PIM
m_DW_BUDGET_Master << m_DW_CURRENCY
m_DW_BUDGET_Master << m_DW_DEPARTMENT_LFL
m_DW_BUDGET_Master << m_DW_Finance_Dimensions
m_DW_C4S_SCORECARD_MASTER << m_DW_RETAIL_COLLECT_AT_STORE
m_DW_C4S_SCORECARD_MASTER << m_DW_PO_RETAIL
m_DW_C4S_SCORECARD_MASTER << m_DW_LOGISTIC_Master
m_DW_C4S_SCORECARD_MASTER << m_DW_COMPENSATION
m_DW_C4S_SCORECARD_MASTER << m_DW_REPAIR_ORDER
m_DW_C4S_SCORECARD_MASTER << m_DW_Stock_Master
m_DW_CAMPAIGN << m_DW_PIM
m_DW_CAMPAIGN_REFERENCES << m_DW_CURRENCY
m_DW_CAMPAIGN_REFERENCES << m_DW_WHS_DIMENSIONS
m_DW_CC365_MASTER << m_DW_NGR_SO_MAIN
m_DW_CLAIMS << m_DW_Stock_Master
m_DW_COMMERCIAL_HUB_MASTER << m_DW_Stock_Master
m_DW_COMMERCIAL_HUB_MASTER << m_DW_STOCK_WHS_GL
m_DW_COMMERCIAL_RETUR << m_DW_Stock_Master
m_DW_COMPENSATION << m_DW_NGR_SO_MAIN
m_DW_CURRENCY << m_DW_TIME
m_DW_CUSTOMER << m_DW_NGR_SO_MAIN
m_DW_CUSTOMER_EXCLUSION << m_DW_NGR_SO_MAIN
m_DW_CUSTOMER_RECRUITMENT << m_DW_NGR_SO_MAIN
m_DW_CUSTOMER_RETURN << m_DW_REPAIR_ORDER
m_DW_CUSTOMER_SATISFACTION << m_DMF_TM
m_DW_CUSTOMER_SATISFACTION << m_DW_REPAIR_ORDER
m_DW_CUSTOMER_SATISFACTION << m_DW_CC365_MASTER
m_DW_DEPARTMENT_LFL << m_DW_TIME
m_DW_DIGITAL_SIGNATURE_TAB << m_DW_NGR_SO_MAIN
m_DW_DIMENSIONS << m_DW_IDENTITY_TAB_MASTER
m_DW_DMF_LAUNCHPAD_MESSAGE << m_DW_TIME
m_DW_DMF_LIFECYCLE << m_DW_TIME
m_DW_DMF_SALE_B2B_TAB << m_DW_PARTNER_B2B_TAB
m_DW_DMF_SALE_B2B_TAB << m_DW_NGR_SO_MAIN
m_DW_DMF_SALE_ORDER_LINE_AGG_temporary << m_DW_NGR_SO_MAIN
m_DW_DMF_SALE_OTIF_TAB << m_DW_RFC
m_DW_DMF_SALE_OTIF_TAB << m_DW_DMF_SALE_RETURN_ORDER_TAB
m_DW_DMF_SALE_OTIF_TAB << m_DW_STOCK
m_DW_DMF_SALE_OTIF_TAB << m_DW_DMF_WHS_PO_CHANGE_TAB
m_DW_DMF_SALE_RETAIL_CAS_TAB << m_DW_NGR_SO_MAIN
m_DW_DMF_SALE_RETURN_ORDER_TAB << m_DW_NGR_SO_MAIN
m_DW_DMF_SALE_RETURN_ORDER_TAB << m_DW_ORDERTYPE
m_DW_DMF_STORE_OPENING_HOURS_TAB << m_DW_TIME
m_DW_DMF_SUPPLIER_FUNDING_METRICS_TAB << m_Start
m_DW_DMF_WHS_PO_CHANGE_TAB << m_DW_DMF_WHS_PO_LINE_TAB
m_DW_DMF_WHS_PO_LINE_TAB << m_DW_STOCK_WHS_GL
m_DW_DMF_WHS_PO_LINE_TAB << m_DW_Stock_Master
m_DW_DMF_WHS_SO_LINE_TAB << m_DW_STORAGELOCATION
m_DW_DMF_WHS_SO_LINE_TAB << m_DW_CURRENCY
m_DW_DeleteMe_Master << m_DW_EMPLOYEE
m_DW_DeleteMe_Master << m_DW_PERMISSION_TAB_MASTER
m_DW_DeleteMe_Master << m_DW_IDENTITY_TAB_MASTER
m_DW_ECOM_MOSTPOPULAR_Master << m_DW_NGR_SO_MAIN
m_DW_EMPLOYEE << m_DW_ORGANIZATION
m_DW_EPOQ_SALES << m_DW_DMF_SALE_OTIF_TAB
m_DW_EXCEPTION_FRE << m_DW_PIM
m_DW_FAULT_RATE << m_DW_RFC
m_DW_FI_CHEMICAL_TAX << m_DW_PIM
m_DW_FI_INVOICE_COCKPIT << m_DW_Finance_Dimensions
m_DW_FI_INVOICE_COCKPIT << m_DW_PO_RETAIL
m_DW_FORECAST_FRE_HIST << m_DW_DMF_WHS_PO_LINE_TAB
m_DW_FULL_RANGE << m_DW_PRICE_TAB
m_DW_FULL_RANGE << m_DW_ARTICLE_ROLE
m_DW_Finance << m_DW_CURRENCY
m_DW_Finance << m_DW_DEPARTMENT_LFL
m_DW_Finance << m_DW_Finance_Dimensions
m_DW_Finance_Dimensions << m_DW_PROFITCENTER
m_DW_HON_AFTERSALE << m_DW_NGR_SO_MAIN
m_DW_HR_PEOPLEPORTAL_MASTER << m_DW_TIME
m_DW_HR_QUINYX_Master << m_DW_CURRENCY
m_DW_IA_EMPLOYEE << m_DW_ORGANIZATION
m_DW_IDENTITY_TAB_MASTER << m_Start
m_DW_INSURANCE << m_DW_CURRENCY
m_DW_INTAKE << m_DW_CURRENCY
m_DW_KPI_REPORTING << m_DW_BUDGET_Master
m_DW_KPI_REPORTING << m_DW_Stock_Master
m_DW_LOGISTIC_Master << m_DW_NGR_SO_MAIN
m_DW_MARKETING_SALES << m_DW_NGR_SO_MAIN
m_DW_MASTERSUPPLIER << m_Start
m_DW_MATLOC << m_DW_DMF_WHS_PO_LINE_TAB
m_DW_MATLOC << m_DW_ARTICLE_ROLE
m_DW_NGR_SO_MAIN << m_DW_DEPARTMENT_LFL
m_DW_NGR_SO_MAIN << m_DW_CURRENCY
m_DW_ODS_ZFRE_SLS_HIST_IF_TAB << m_DW_NGR_SO_MAIN
m_DW_ORDERTYPE << m_Start
m_DW_ORGANIZATION << m_DW_PROFITCENTER
m_DW_OUTSTANDING_AMOUNT_RFC << m_DW_CURRENCY
m_DW_PARTNER_B2B_TAB << m_DW_DIMENSIONS
m_DW_PARTNER_B2B_TAB << m_DW_DeleteMe_Master
m_DW_PARTNER_B2B_TAB << m_DW_POS_ORDER_UPDATE_TAB
m_DW_PAYMENT << m_DW_NGR_SO_MAIN
m_DW_PAYMENT << m_DW_IA_EMPLOYEE
m_DW_PERMISSION_TAB_MASTER << m_DW_ORGANIZATION
m_DW_PIM << m_DW_EMPLOYEE
m_DW_PIM << m_DW_MASTERSUPPLIER
m_DW_PIM_ARTICLECONTENT << m_Start
m_DW_POS_ORDER_UPDATE_TAB << m_DW_IDENTITY_TAB_MASTER
m_DW_PO_RETAIL << m_DW_CURRENCY
m_DW_PO_RETAIL << m_DW_ORDERTYPE
m_DW_PO_RETAIL << m_DW_WHS_DIMENSIONS
m_DW_PRICE_CALCULATION << m_DW_CURRENCY
m_DW_PRICE_TAB << m_DW_CURRENCY
m_DW_PROFITCENTER << m_Start
m_DW_PnP << m_DW_Finance
m_DW_PnP << m_DW_BUDGET_Master
m_DW_QUINYX_Master << m_DW_NGR_SO_MAIN
m_DW_REPAIR_ORDER << m_DW_TM_NEW
m_DW_RETAIL_COLLECT_AT_STORE << m_DW_NGR_SO_MAIN
m_DW_RETAIL_COLLECT_AT_STORE << m_DW_DMF_STORE_OPENING_HOURS_TAB
m_DW_RFC << m_DW_IA_EMPLOYEE
m_DW_RFC << m_DW_CUSTOMER_RETURN
m_DW_RFC << m_DW_WHS_MARGIN
m_DW_RFC_CURRENT_WHS << m_DW_RFC
m_DW_SALE << m_DW_PAYMENT
m_DW_SALE_ATTACHMENT << m_DW_NGR_SO_MAIN
m_DW_SALE_BLUEBERRY_BASKET_MASTER << m_DW_CURRENCY
m_DW_SALE_BLUEBERRY_BASKET_MASTER << m_DW_IA_EMPLOYEE
m_DW_SALE_BRIDGE_ORDER_TAB_MASTER << m_DW_DMF_WHS_PO_CHANGE_TAB
m_DW_SALE_CANCEL_ORDER << m_DW_NGR_SO_MAIN
m_DW_SALE_COMMERCIAL_SERVICES_HIT_RATE << m_DW_NGR_SO_MAIN
m_DW_SALE_DOCUMENT_MASTER << m_DW_CURRENCY
m_DW_SALE_RECURRING_SERVICES << m_DW_NGR_SO_MAIN
m_DW_SALE_SUBSCRIPTION_SELECTOR << m_DW_NGR_SO_MAIN
m_DW_SEGMENTATION_MASTER << m_DW_DeleteMe_Master
m_DW_SOLD_NOT_DELIVERED << m_DW_NGR_SO_MAIN
m_DW_STOCK << m_DW_DMF_WHS_PO_LINE_TAB
m_DW_STOCK_ATP7 << m_DW_DMF_LIFECYCLE
m_DW_STOCK_ATP7 << m_DW_ABC_CLASSIFICATION
m_DW_STOCK_Dimensions << m_DW_WHS_DIMENSIONS
m_DW_STOCK_WHS_GL << m_DW_CURRENCY
m_DW_STOCK_WHS_GL << m_DW_STOCK_Dimensions
m_DW_STORAGELOCATION << m_DW_STOCK_Dimensions
m_DW_SUPPLIER_FUNDING_CONTRACT << m_DW_NGR_SO_MAIN
m_DW_SURVEY << m_Start
m_DW_Stock_Master << m_DW_ASSORTMENT_TAB
m_DW_Stock_Master << m_DW_IA_EMPLOYEE
m_DW_TIME << m_ODS_SAP_FULL_RANGE
m_DW_TIME << m_DW_DIMENSIONS
m_DW_TIME << m_DW_DeleteMe_Master
m_DW_TIME << m_DW_POS_ORDER_UPDATE_TAB
m_DW_TIME << m_DW_CAMPAIGN
m_DW_TM_NEW << m_DW_HON_AFTERSALE
m_DW_TRADE_IN << m_DW_CURRENCY
m_DW_TRANSPORT << m_DMF_TM
m_DW_TRANSPORT_DIM << m_DW_ORGANIZATION
m_DW_TRANSPORT_INSURANCE << m_DW_Stock_Master
m_DW_UNBOOKED_RETURNSTORE << m_DW_CURRENCY
m_DW_WHS_DIMENSIONS << m_Start
m_DW_WHS_MARGIN << m_DW_DMF_WHS_SO_LINE_TAB
m_DW_WHS_MARGIN << m_DW_NGR_SO_MAIN
m_DW_WHS_MARGIN << m_DW_ORDERTYPE
m_DW_WHS_MARGIN << m_DW_Finance_Dimensions
m_DW_WORK_TICKET << m_DW_TIME
m_DW_dmf_article_price_competitorinto_tab_master << m_DW_BUDGET_Master
m_DW_dmf_article_price_competitorinto_tab_master << m_DW_PRICE_TAB
m_DW_dmf_article_price_competitorinto_tab_master << m_DW_STOCK
m_EMPLOYEE_DISCOUNT_master << m_Start
m_End << m_DW_SUPPLIER_FUNDING_CONTRACT
m_End << m_DW_TRANSPORT
m_End << m_DW_dmf_article_price_competitorinto_tab_master
m_End << m_DW_C4S_SCORECARD_MASTER
m_End << m_DW_FAULT_RATE
m_End << m_DW_FORECAST_FRE_HIST
m_End << m_DW_FULL_RANGE
m_End << m_DW_RFC_CURRENT_WHS
m_End << m_DW_DMF_SALE_B2B_TAB
m_End << m_DW_SEGMENTATION_MASTER
m_End << m_DW_HR_PEOPLEPORTAL_MASTER
m_End << m_DW_KPI_REPORTING
m_End << m_DW_HR_QUINYX_Master
m_End << m_DW_INTAKE
m_End << m_DW_MATLOC
m_End << m_DW_SALE_BRIDGE_ORDER_TAB_MASTER
m_End << m_DW_CUSTOMER_EXCLUSION
m_End << m_ZSD_STORESPLIT
m_End << m_DW_CAMPAIGN_REFERENCES
m_End << m_DW_OUTSTANDING_AMOUNT_RFC
m_End << m_DW_PRICE_CALCULATION
m_End << m_DW_QUINYX_Master
m_End << m_DW_COMMERCIAL_HUB_MASTER
m_End << m_DW_SALE_RECURRING_SERVICES
m_End << m_DW_DMF_SALE_RETAIL_CAS_TAB
m_End << m_DW_SOLD_NOT_DELIVERED
m_End << m_OVERSTOCK
m_End << m_DW_DMF_SUPPLIER_FUNDING_METRICS_TAB
m_End << m_DW_DMF_SALE_ORDER_LINE_AGG_temporary
m_End << m_DW_TRANSPORT_INSURANCE
m_End << m_DW_CUSTOMER
m_End << m_DW_SALE
m_End << m_DW_SALE_ATTACHMENT
m_End << m_DW_MARKETING_SALES
m_End << m_MARKETING_PLAN_Master
m_End << m_DW_ODS_ZFRE_SLS_HIST_IF_TAB
m_End << m_DW_COMMERCIAL_RETUR
m_End << m_DW_CUSTOMER_RECRUITMENT
m_End << m_MIRAKL_Master
m_End << m_DW_SALE_SUBSCRIPTION_SELECTOR
m_End << m_DW_TRADE_IN
m_End << m_DW_SALE_CANCEL_ORDER
m_End << m_DW_SALE_BLUEBERRY_BASKET_MASTER
m_End << m_DW_EXCEPTION_FRE
m_End << m_DW_EPOQ_SALES
m_End << m_DW_SALE_COMMERCIAL_SERVICES_HIT_RATE
m_End << m_DW_CLAIMS
m_End << m_DW_BAZAARVOICE_Master
m_End << m_DW_PnP
m_End << m_DW_ECOM_MOSTPOPULAR_Master
m_End << m_DW_UNBOOKED_RETURNSTORE
m_End << m_DW_AccountReceivables
m_End << m_DW_CUSTOMER_SATISFACTION
m_End << m_DW_WORK_TICKET
m_End << m_DW_APPOINTMENTS
m_End << m_DW_SALE_DOCUMENT_MASTER
m_End << m_DW_DMF_LAUNCHPAD_MESSAGE
m_End << m_DW_DIGITAL_SIGNATURE_TAB
m_End << m_DW_FI_CHEMICAL_TAX
m_End << m_DW_FI_INVOICE_COCKPIT
m_End << m_DW_SURVEY
m_End << m_DW_ACADEMY_MASTER
m_End << m_DW_PIM_ARTICLECONTENT
m_End << m_EMPLOYEE_DISCOUNT_master
m_End << m_DW_INSURANCE
m_IA_SAP_HIST_TABLES << m_Start
m_MARKETING_PLAN_Master << m_DW_Finance
m_MARKETING_PLAN_Master << m_DW_NGR_SO_MAIN
m_MIRAKL_Master << m_DW_NGR_SO_MAIN
m_ODS_SAP_FULL_RANGE << m_DW_PIM
m_OVERSTOCK << m_DW_DMF_WHS_PO_LINE_TAB
m_OVERSTOCK << m_DW_BUDGET_Master
m_ZSD_STORESPLIT << m_DW_NGR_SO_MAIN