from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import pandas_datareader.data as web

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def prepareData():
    print('hoi')
    stock_list = ['AAPL','GOOGL','AMZN','TSLA','FB','ROG.SW','NOVN.SW','IDIA.SW','CSGN.SW','UBSG.SW','RLF.SW','SEDG','MDB','ALGN','ALXN','SIVB','MBTN.SW','PGHN.SW','NESN.SW','ABBV','AYX','ADS']
    result = pd.DataFrame(columns=('Date', 'Stock', 'Action'))
    end = datetime.now()
    start = datetime(end.year - 1,end.month,end.day)#
    for stock in stock_list:   
        globals()[stock] = web.DataReader(stock,'yahoo',start,end)
    for stock in stock_list:
        df = globals()[stock]
        df['Close: 30 Day Mean'] = df['Close'].rolling(window=20).mean()
        df['Upper'] = df['Close: 30 Day Mean'] + 2*df['Close'].rolling(window=20).std()
        df['Lower'] = df['Close: 30 Day Mean'] - 2*df['Close'].rolling(window=20).std()
        fig = df[['Close','Close: 30 Day Mean','Upper','Lower']].plot(figsize=(16,6),title=stock).get_figure()
        fig.savefig(stock + '.png')
        dfTail = df.tail(1)
        date = pd.to_datetime(dfTail.iloc[0].name)
        close = dfTail.iloc[0]['Close']
        upper = dfTail.iloc[0]['Upper']
        lower = dfTail.iloc[0]['Lower']
        diffrence = end - date
        action = 'hold'
        if diffrence.days < 3:
            if close < lower:
                action='buy'
            elif close > upper:
                action='sell'
            else:
                pass
        else:
            print('Date aelter als drei Tage!!!')
        result = result.append({'Date' : date , 'Stock' : stock, 'Action': action,'Close':close}, ignore_index=True)
#    print(result)
#    result.to_csv('stocks.csv', index=False)
    return str(result)

def get_pushed_xcom_with_return(**context):
    print(context['ti'].xcom_pull(task_ids='getDataFromYahoo')) 

with DAG("hoi", start_date=datetime(2021, 1 ,1), 
    schedule_interval="@daily", default_args=default_args, catchup=False) as dag:


    getDataFromYahoo = PythonOperator(
        task_id="getDataFromYahoo",
        python_callable=prepareData,
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="reto.schuermann@gmail.com",
        subject="Hoi",
        html_content= "<h3>hoi</h3><br> {{ ti.xcom_pull(task_ids='getDataFromYahoo') }} ",
#        files=['stocks.csv']
    )

    getDataFromYahoo >> send_email_notification