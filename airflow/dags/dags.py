import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

import airflow.utils
import airflow.utils.dates

from airflow_etl_pipeline import *

def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    transformed_df = transform_avg_ratings(movies_df, users_df)
    load_df_to_db(transformed_df)
    print(transformed_df.show())

default_args = {
    'owner': 'deepanshu',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past':True,
    'email':['2207deepanshu@gmail.com'],
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':3,
    'retry_delay':timedelta(minutes=5),

}
# dag = DAG(dag_id='etl_pipeline',
#           default_args=default_args,
#           schedule_interval='@hourly')
dag = DAG(dag_id='deep',
          default_args=default_args,
          schedule_interval="20 21 * * *")


etl_task = PythonOperator(task_id="deep_etl",
                          python_callable = etl,
                          dag = dag)
# etl()