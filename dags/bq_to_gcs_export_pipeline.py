import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from dags.common.query_utils import query_utils
from dags.common.utils import get_default_args
from dags.dag_configs.file_export_configs import file_export_configs
from dags.dag_configs.variables import *

query_utils = query_utils()

def fetch_last_exported_date(**kwargs):
    return

def check_record_count(**kwargs):
    return

def insert_cdc_details(**kwargs):
    return

def create_dag(config):
    default_args = get_default_args(retries=1)

    dag_id = "{}_export".format(config['source_table_name'])

    return DAG(dag_id=dag_id,
               default_args=default_args,
               schedule_interval= config['schedule_interval'],
               tags=["bq_gcs_export"]
            )

for export_config in file_export_configs:
    dag = create_dag(export_config)
    globals()[dag.dag_id] = dag

    # variables
    source_table_name = export_config['source_table_name']
    export_format = export_config['export_format']
    load_type = export_config['load_type']
    cdc_column_name = export_config['cdc_column'] if load_type == "DELTA" else ''

    # start building task
    start_export = DummyOperator(task_id="start_task", dag=dag)
    end_export = DummyOperator(task_id="end_task", dag=dag)

    # Step1: Move files from active to archive
    move_file_archive = GCSToGCSOperator(
        task_id="move_to_archive",
        source_bucket=GCP_SHARED_DESTINATION_BUCKET_NAME,
        source_object="active/" + source_table_name,
        destination_bucket=GCP_SHARED_DESTINATION_BUCKET_NAME,
        destination_object="archive/" + source_table_name,
        move_object=True,
        gcp_conn_id=GCP_CONN_STRING,
        dag=dag
    )

    # Step2: Get last export date from CDC master for source table
    fetch_last_export_date = PythonOperator(
        task_id="fetch_last_export_date",
        python_callable=fetch_last_exported_date,
        op_kwargs={
            "google_cloud_conn_id": GCP_CONN_STRING,
            "query": """SELECT MAX(last_export_datetime) as last_export_datetime FROM output.change_data_capture_audit 
            WHERE source_table_name=@source_table_name""",
            "source_table_name": source_table_name
        },
        provide_context=True,
        dag=dag
    )

    check_count = PythonOperator(
        task_id="check_count",
        python_callable=check_record_count,
        op_kwargs={
            "google_cloud_conn_id": GCP_CONN_STRING,
            "source_table_name": source_table_name,
            "load_type": load_type,
            "cdc_column": cdc_column_name
        },
        provide_context=True,
        dag=dag
    )

    export_bq_to_gcs = BigQueryInsertJobOperator(
         task_id='export_bq_to_gcs',
         configuration={
             "query": {
                 "query": query_utils.create_cdc_export_query(
                     PROJECT_ID,
                     GCP_CONN_STRING,
                     source_table_name,
                     cdc_column_name,
                     export_format
                 ),
                 "allow_large_results": True,
                 "useLegacySql": False,
             }
         },
         gcp_conn_id=GCP_CONN_STRING,
         dag=dag
     )

    update_cdc_master = PythonOperator(
        task_id='update_cdc_master',
        python_callable=insert_cdc_details,
        op_kwargs={
            "google_cloud_conn_id": GCP_CONN_STRING,
            "source_table_name": source_table_name,
            "load_type": load_type,
            "cdc_column": cdc_column_name

        },
        provide_context=True,
        do_xcom_push=False,
        dag=dag
    )

    start_export >> move_file_archive >> fetch_last_export_date >> check_count >> export_bq_to_gcs >> update_cdc_master >> end_export
