#import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#Define Dag argument
default_args = {
    'owner': 'me',
    'start_date': days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#Define the DAG
dag=DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='This is my first_etl DAG',
    schedule_interval=timedelta(days=1),
)
# DEFINE THE TASKS
# define the task 'Download'

download=BashOperator(
    task_id='download',
    bash_command = 'wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag=dag,
)

# define the 'extract' task
#The extract task must extract the fields timestamp and visitorid
extract= BashOperator(
    task_id='extract',
    bash_command = 'cut -f1,4 -d"#" web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag=dag,
)
#define the 'transform' task
# The transform task must capitalize the visitorid
transform= BashOperator(
    task_id='transform',
    bash_command = 'tr "[a-z]" "[A-Z]"< /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt',
    dag=dag,
)
# define the 'load' task
# The load task must compress the extracted and transformed data.

load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt' ,
    dag=dag,
)

#task pipeline

download >> extract >> transform >> load

