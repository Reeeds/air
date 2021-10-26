from datetime import datetime, timedelta
import pandas as pd
import json
import io
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}

google_cloud_connection_id = 'google_cloud_default'
postgres_connection_id = 'postgresConfigDB'


def getDataFromPostgres():
        query = '''
            select analysen.id as Analyse_id,customers.id as Customer_id, "paramList"
            from customers
            inner join ht
            on customers.id = ht.fk_customer_id
            inner join analysen
            on analysen.id = ht.fk_analyse_id
            order by analysen.id,customers.id
            ;
        '''
        pg_hook = PostgresHook(postgre_conn_id=postgres_connection_id,schema="public")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(query)
        sources = cursor.fetchall()
        print(sources)
        return sources

@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['config'])
def config():
    start_task = DummyOperator(task_id='start_task')
    hook_task = PythonOperator(task_id='hook_task',python_callable=getDataFromPostgres)
    start_task >> hook_task

#    @task()
#    def getDataFromPostgres():
#        get_config = PostgresOperator(task_id="get_config", sql=query)
#        print(get_config)


#    @task()
#    def uploadData(data):
##        output = pd.read_csv(filepath_or_buffer=io.StringIO(data),encoding='utf8', sep=';')
#        gcs_hook = GCSHook(
#            gcp_conn_id=google_cloud_connection_id
#        )
#        gcs_hook.upload(bucket_name='pre_bucket', data=data, object_name='output.csv', mime_type='application/csv')

