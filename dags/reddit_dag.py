# dag.py
import os
from airflow import DAG
from datetime import timedelta, datetime, date, time

path = os.environ['AIRFLOW_HOME']

from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from reddit_etl import main

#  default configuration options for the DAG
default_args = {
	'owner': 'ECairflowreddit',
	'depends_on_past': False, #Whether or not the DAG should consider past runs when determining if the current run should be executed.
	'start_date': datetime(2021, 7, 29, 2), #The date and time when the DAG should start running
	'email': ['emanuelcalderon071@gmail.com'], #A list of email addresses that should receive notifications about the DAG's execution.
	'email_on_failure': True, #Whether or not to send an email notification if the DAG fails.
	'email_on_retry': False, #same but for retried
	'retries': 2, #The number of times to retry the DAG if it fails
	'retry_delay': timedelta(minutes=2), #The amount of time to wait before retrying the DAG if it fails
    'schedule_interval':"*/5 * * * *"
}

dag = DAG(
	default_args=default_args, #Passing the default_args dictionary to the DAG constructor
	description='A simple ETL DAG',
    catchup=False,
    dag_id='reddit_dag'
)

# Define the task 1 (collect the data) id. Run the bash command because the task is in a .py file.
# task1 = BashOperator(
#                         task_id='get_data',
#                         bash_command=f'python {path}/dags/src/reddit_etl.py',
#                         dag=dag
#                     )

# # Define Task 2 (insert the data into the database)
# task2 = BashOperator(
#                      task_id='insert_data',
#                      bash_command=f'python {path}/dags/src/insert_data.py'
#                     )
# # task2 will only run after task1 is finished.
# task1 >> task2

run_etl = PythonOperator(
	task_id='complete_reddit_etl', #name of the task
	python_callable=main, #the function that will be executed
    # op_kwargs={'client_id': client_id},  # ← passing the secret here
	dag=dag, #Passing the dag instance to the PythonOperator constructor
)