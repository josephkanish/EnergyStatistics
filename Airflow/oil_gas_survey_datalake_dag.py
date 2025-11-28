# dags/oil_gas_survey_datalake_dag.py

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor

# ---------- Config ----------
DAG_ID = "oil_gas_survey_datalake_dag"

RAW_BASE_PATH = "/data/raw/oil_gas_statistics"   # adapt to your storage mount
SPARK_APP_BASE = "/opt/etl/oil_gas"              # folder with PySpark scripts

default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval="@yearly",     # or None for manual
    catchup=False,
    description="End-to-end data lake pipeline for Oil & Gas survey Excel (RAW -> BI)",
    max_active_runs=1,
) as dag:

    # 0. Start
    start = EmptyOperator(task_id="start")

    # 1. Wait for RAW file for this year
    # If RAW is on local/posix mount; for S3/ADLS use an appropriate sensor instead.
    wait_for_raw_file = FileSensor(
        task_id="wait_for_raw_file",
        filepath=(
            f"{RAW_BASE_PATH}/"
            "{{ execution_date.year }}/"
            "Oil_and_Gas_Statistics_{{ execution_date.year }}_En.xlsx"
        ),
        poke_interval=60 * 10,
        timeout=60 * 60 * 12,
        mode="reschedule",
    )

    # ---------- SparkSubmit tasks ----------
    # Each script should accept: --year {{ execution_date.year }}

    raw_to_staging = SparkSubmitOperator(
        task_id="raw_to_staging",
        application=f"{SPARK_APP_BASE}/raw_to_staging.py",
        name="raw_to_staging_oil_gas",
        application_args=[
            "--year", "{{ execution_date.year }}",
            "--raw-base-path", RAW_BASE_PATH,
            "--staging-db", "stg_db",
        ],
        conn_id="spark_default",  # adapt to your Spark connection
        verbose=True,
    )

    staging_to_l0 = SparkSubmitOperator(
        task_id="staging_to_l0",
        application=f"{SPARK_APP_BASE}/staging_to_l0.py",
        name="staging_to_l0_oil_gas",
        application_args=[
            "--year", "{{ execution_date.year }}",
            "--staging-db", "stg_db",
            "--l0-db", "l0_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    l0_to_l1 = SparkSubmitOperator(
        task_id="l0_to_l1",
        application=f"{SPARK_APP_BASE}/l0_to_l1.py",
        name="l0_to_l1_oil_gas",
        application_args=[
            "--year", "{{ execution_date.year }}",
            "--l0-db", "l0_db",
            "--l1-db", "l1_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    l1_to_agg = SparkSubmitOperator(
        task_id="l1_to_agg",
        application=f"{SPARK_APP_BASE}/l1_to_agg.py",
        name="l1_to_agg_oil_gas",
        application_args=[
            "--year", "{{ execution_date.year }}",
            "--l1-db", "l1_db",
            "--agg-db", "agg_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    agg_to_bi_dims = SparkSubmitOperator(
        task_id="agg_to_bi_dims",
        application=f"{SPARK_APP_BASE}/agg_to_bi_dims.py",
        name="agg_to_bi_dims_oil_gas",
        application_args=[
            "--agg-db", "agg_db",
            "--bi-db", "bi_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    agg_to_bi_fact = SparkSubmitOperator(
        task_id="agg_to_bi_fact",
        application=f"{SPARK_APP_BASE}/agg_to_bi_fact.py",
        name="agg_to_bi_fact_oil_gas",
        application_args=[
            "--agg-db", "agg_db",
            "--bi-db", "bi_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    data_quality_checks = SparkSubmitOperator(
        task_id="data_quality_checks",
        application=f"{SPARK_APP_BASE}/bi_quality_checks.py",
        name="bi_quality_checks_oil_gas",
        application_args=[
            "--bi-db", "bi_db",
        ],
        conn_id="spark_default",
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # ---------- Dependencies ----------
    start >> wait_for_raw_file >> raw_to_staging
    raw_to_staging >> staging_to_l0 >> l0_to_l1 >> l1_to_agg
    l1_to_agg >> [agg_to_bi_dims, agg_to_bi_fact]
    [agg_to_bi_dims, agg_to_bi_fact] >> data_quality_checks >> end
