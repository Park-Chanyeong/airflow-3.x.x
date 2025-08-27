import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Variable





with DAG(
    dag_id="dags_bash_with_variable",
    schedule="10 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 이렇게 하면 airflow 스케줄러가 계속 이걸 파싱해서 한번씩 실행시키기 때문에 부하가 옴
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
    task_id="bash_var_1",
    bash_command=f"echo variable:{var_value}"
    )
    # 이렇게 variable에서 가져와서 오퍼레이터 안에 있는 경우에는 파싱안하기 때문에 airflow에서도 이 방법을 권고
    bash_var_2 = BashOperator(
    task_id="bash_var_2",
    bash_command="echo variable:{{var.value.sample_key}}"
    )