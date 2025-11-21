# airflow/dags/eHealth_pipeline.py
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "eHealth",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eHealth_pipeline",
    default_args=default_args,
    description="eHealth CDS pipeline: schema, synthetic data, NIH ingestion, warehouse, analytics",
    schedule_interval="@daily",  # or "@hourly"
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    project_root = "/app"
    part1_dir = os.path.join(project_root, "part1")
    part2_dir = os.path.join(project_root, "part2_pipeline")
    part3_dir = os.path.join(project_root, "part3_analytics")

    create_schema = BashOperator(
        task_id="create_schema",
        bash_command=f"python {part1_dir}/create_schema.py",
    )

    generate_synthetic = BashOperator(
        task_id="generate_synthetic_data",
        bash_command=f"python {part1_dir}/generate_synthetic_data.py",
    )

    extract_nih = BashOperator(
        task_id="extract_nih_dataset",
        bash_command=f"python {part2_dir}/extract_nih_dataset.py",
    )

    generate_reports = BashOperator(
        task_id="generate_synthetic_reports",
        bash_command=f"python {part2_dir}/generate_synthetic_reports.py",
    )

    ingest_nih = BashOperator(
        task_id="ingest_nih_pipeline",
        bash_command=f"python {part2_dir}/etl_pipeline.py",
    )

    populate_warehouse = BashOperator(
        task_id="populate_warehouse",
        bash_command=f"python {part3_dir}/populate_warehouse.py",
    )

    run_analytics = BashOperator(
        task_id="run_analytics",
        bash_command=f"python {part3_dir}/run_analytics.py",
    )
    
    qa_checks = BashOperator(
        task_id="warehouse_qa_checks",
        bash_command="python /app/part3_analytics/run_warehouse_qa.py",
    )

    create_schema >> generate_synthetic >> extract_nih >> generate_reports >> ingest_nih >> populate_warehouse >> run_analytics >> qa_checks
