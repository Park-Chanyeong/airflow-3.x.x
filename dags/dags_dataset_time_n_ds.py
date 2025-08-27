from airflow.timetables.trigger import CronTriggerTimetable
import pendulum
from airflow.sdk import DAG, task, Asset
from airflow.timetables.assets import AssetOrTimeSchedule

with DAG(
        dag_id='dags_dataset_time_n_ds',
        schedule=AssetOrTimeSchedule(
            timetable=CronTriggerTimetable("* * * * *", timezone="Asia/Seoul"),
            assets=(Asset('dags_dataset_producer_3') &
                      (Asset("dags_dataset_producer_1") | Asset("dags_dataset_producer_2"))
            )
        ),
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        catchup=False,
        tags=['update:2.10.5','asset','consumer']
) as dag:
    @task.bash(task_id='task_bash')
    def task_bash():
        return 'echo "schedule run"'

    task_bash()