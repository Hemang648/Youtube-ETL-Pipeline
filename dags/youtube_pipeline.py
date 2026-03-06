from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="youtube_etl_pipeline_v1.0",
    start_date=datetime(2026,1,1),
    schedule="@daily",
    catchup=False
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="python /opt/airflow/scripts/extract.py"
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="python /opt/airflow/scripts/transform.py"
    )

    load = BashOperator(
        task_id="load",
        bash_command="python /opt/airflow/scripts/load.py"
    )

    extract >> transform >> load