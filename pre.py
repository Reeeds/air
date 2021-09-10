
from datetime import datetime, timedelta
import pandas as pd
#import os 
#import glob
import io
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import association_rules, apriori
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
#from airflow.utils.email import send_email
from airflow.models import Variable


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


@dag(default_args=default_args, schedule_interval="* * * * *", start_date=days_ago(2), tags=['example'])
def pre():

    @task()
    def extractData():

        dfDataSalDocsTest = Variable.get("dfDataSalDocsTestJSON", deserialize_json=True)

        dfDataSalDocs = pd.DataFrame(dfDataSalDocsTest)

        dfDataSalDocs = dfDataSalDocs.groupby('SalDoc_InternalNo')['SalDocItem_ArtInternalNo']
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
        return df.to_csv()

    @task()
    def transform2(data):
        aResult = pd.read_csv(io.StringIO(data))
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
        dfResult['ConnectedArt_BoId'] = str(dfResult['consequents']) + "," +   str("70")      + "," + str(dfResult['antecedents'])
        print(dfResult.head())
        return dfResult.to_csv()


    data = extractData()
    transformedData = transform1(data)
    transform2(transformedData)

pre = pre()

