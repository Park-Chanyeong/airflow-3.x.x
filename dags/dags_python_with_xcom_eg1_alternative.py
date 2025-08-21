import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_xcom_eg1_alternative",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    def xcom_push1(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_1")
        ti.xcom_push(key="result2", value=[1,2,3])

    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key="result1", value="value_2")
        ti.xcom_push(key="result2", value=[1,2,3,4])

    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(key="result1", task_ids=['python_xcom_push_task1','python_xcom_push_task2'])
        value2 = ti.xcom_pull(key="result2", task_ids='python_xcom_push_task1')
        print(value1)
        print(value2)

    task1 = PythonOperator(
        task_id='python_xcom_push_task1',
        python_callable=xcom_push1
    )
    
    task2 = PythonOperator(
        task_id='python_xcom_push_task2',
        python_callable=xcom_push2
    )
    
    task3 = PythonOperator(
        task_id='python_xcom_pull_task',
        python_callable=xcom_pull
    )

    task1 >> task2 >> task3
