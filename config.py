from datetime import datetime, timedelta
import pandas as pd
import json
import io
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

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


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['config'])
def config():

    @task()
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
        get_config = PostgresOperator(task_id="get_config", sql=query)
        print(get_config)


#    @task()
#    def uploadData(data):
##        output = pd.read_csv(filepath_or_buffer=io.StringIO(data),encoding='utf8', sep=';')
#        gcs_hook = GCSHook(
#            gcp_conn_id=google_cloud_connection_id
#        )
#        gcs_hook.upload(bucket_name='pre_bucket', data=data, object_name='output.csv', mime_type='application/csv')

    getDataFromPostgres()
config = config()