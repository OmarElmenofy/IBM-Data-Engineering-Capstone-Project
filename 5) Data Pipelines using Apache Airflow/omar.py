from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
default_args = {
    'owner': 'omarelmenofy',
    'start_date': days_ago(0),
    'email': ['omar1@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# define the DAG
dag = DAG(
    dag_id='process_web_log',
    default_args=default_args,
    description='process_web_log using Bash',
    schedule_interval=timedelta(days=1),
)
# define the tasks

extract = BashOperator(
    task_id='extract_data'
    bash_command= 
    'cut -d " " -f1 /home/project/airflow/dags/capstone/accesslog.txt > /home/project/airflow/dags/capstone/extracted_data.txt'
    dag=dag,
)

# define the second task named transform
transform = BashOperator(
    task_id='transform_data',
    bash_command='grep "198.46.149.143" /home/project/airflow/dags/capstone/extracted_data.txt >  /home/project/airflow/dags/capstone/transform_data.txt',
    dag=dag,
)

# define the third task named load

load = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /home/project/airflow/dags/capstone/weblog.tar /home/project/airflow/dags/capstone/transformed_data.txt ',
    dag=dag,
)
#task pipeline
extract_data >> transform_data >> load_data 