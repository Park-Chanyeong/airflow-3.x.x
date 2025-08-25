import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

asset_dags_dataset_producer_2 = Asset("dags_dataset_producer_2")

with DAG(
        dag_id='dags_dataset_producer_2',
        schedule='0 7 * * *',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['asset','producer']
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        outlets=[asset_dags_dataset_producer_2],
        bash_command='echo "producer_2 수행 완료"'
    )