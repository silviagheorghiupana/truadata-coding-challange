from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

default_args={
    'owner':'silvia',
    'retries':3,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id='DAG_silvia_test',
    default_args = default_args,
    description = 'learning dag 01',
    start_date=datetime(2022,12,15,5),
    schedule_interval='@daily'
) as dag:
    task1 = EmptyOperator(
        task_id ='task-01'
    )
  # Start task group definition
    with TaskGroup(group_id='group1') as tg1:
        task2 = EmptyOperator(
            task_id ='task-02'
        )
        task3 = EmptyOperator(
            task_id ='task-03'
        )
    with TaskGroup(group_id='group2') as tg2:
        task4 = EmptyOperator(
            task_id ='task-04'
        )
        task5 = EmptyOperator(
            task_id ='task-05'
        )
        task6 = EmptyOperator(
            task_id ='task-06'
       )

task1 >> tg1 >> tg2