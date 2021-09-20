from datetime import datetime, timedelta
import pandas as pd
#import os 
#import glob
import json
import io
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules, apriori
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.utils.email import send_email
from airflow.models import Variable
#from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from airflow.utils.db import provide_session
from airflow.models import XCom
from sqlalchemy import func

default_args = {
    'owner': 'airflow',
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 0,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}

minSupport = float(Variable.get("minSupport"))
numberOfRecommendationsPerArt = int(Variable.get("numberOfRecommendationsPerArt"))
google_cloud_connection_id = 'google_cloud_default'


@provide_session
def cleanup_xcom(session=None):
    print('hoi')
    session.query(XCom).filter(XCom.execution_date <= func.date('2019-06-01')).delete(synchronize_session=False)


@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['pre'],on_success_callback=cleanup_xcom)
def pre():

    @task()
    def loadData():
        gcs_hook = GCSHook(
            gcp_conn_id=google_cloud_connection_id
        )
        file = gcs_hook.download(bucket_name='pre_bucket', object_name='dfDataSalDocsTest.csv')
        df = pd.read_csv(filepath_or_buffer=io.BytesIO(file))
        return df.to_csv()

    @task()
    def extractData(data):
        data = pd.read_csv(filepath_or_buffer=io.StringIO(data))
#        dfDataSalDocsTest = Variable.get("dfDataSalDocsTestJSON", deserialize_json=True)
#        dfDataSalDocs = pd.DataFrame(dfDataSalDocsTest)
        print(data.head())
        dfDataSalDocs = data.groupby('SalDoc_InternalNo')['SalDocItem_ArtInternalNo']
        dataSalDocsList = []
        for name, items in dfDataSalDocs:
            basketItems = items.tolist()
            itemsList = []
            for item in basketItems:
                itemsList.append(str(item))
            dataSalDocsList.append(itemsList)
        
        return dataSalDocsList

    @task()
    def transform1(data:list):
        te = TransactionEncoder()
        te_ary = te.fit_transform(data, sparse=True)
        sparse_df = pd.DataFrame.sparse.from_spmatrix(te_ary, columns=te.columns_)
        print('Python Script: SparseMatrix calculated')

        frequent_itemsets = apriori(sparse_df, min_support=minSupport, use_colnames=True, max_len=2, low_memory=True) # LowMemory muss True sein!
        #print(frequent_itemsets)
        print('Python Script: Apriori calculated')

        rules = association_rules(frequent_itemsets, metric='lift', min_threshold=1.0)
        print('Python Script: AssociationRules calculated')

        aResult = rules.sort_values('conviction', ascending=False)
        # Umformatieren
        aResult["antecedents"] = rules["antecedents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
        aResult["consequents"] = rules["consequents"].apply(lambda x: ', '.join(list(x))).astype("unicode")
        df = aResult
        return df.to_csv(encoding='utf8', sep=';')

    @task()
    def transform2(data):
        aResult = pd.read_csv(filepath_or_buffer=io.StringIO(data),encoding='utf8', sep=';')
        allArtDistinct = aResult.antecedents.unique()
        dfResult = pd.DataFrame()
        for artNo in  allArtDistinct:
            dfart = aResult.loc[aResult['antecedents'] == artNo].head(numberOfRecommendationsPerArt)
            val = (dfart.reset_index().index * 10) +10
            dfart = dfart.assign(rang = val)
            dfResult = dfResult.append(dfart)

        # Reihenfolge der Spalten ändern
        dfResult = dfResult[['antecedents', 'consequents','rang','antecedent support','consequent support','support','confidence','lift','leverage','conviction']]
        # Neue Spalte mit BoId von ConnectedArt zusammenbauen
        # todo: gibt einen fehler: dfResult['ConnectedArt_BoId'] = str(dfResult['consequents']) + "," +   str("70")      + "," + str(dfResult['antecedents'])
        print(dfResult.head(50))
        return dfResult.to_csv(encoding='utf8', sep=';')

    @task()
    def uploadData(data):
#        output = pd.read_csv(filepath_or_buffer=io.StringIO(data),encoding='utf8', sep=';')
        gcs_hook = GCSHook(
            gcp_conn_id=google_cloud_connection_id
        )
        gcs_hook.upload(bucket_name='pre_bucket', data=data, object_name='output.csv', mime_type='application/csv')

    data = loadData()
    data = extractData(data)
    transformedData = transform1(data)
    transformedData2 = transform2(transformedData)
    uploadData(transformedData2)
pre = pre()