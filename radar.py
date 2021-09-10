
import requests
from datetime import datetime, timedelta
import urllib.request
from scrapy.selector import Selector

import os 
import glob
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.models import Variable


default_args = {
    'owner': 'airflow',
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "reto.schuermann@gmail.com",
    "retries": 1,
    "execution_timeout": timedelta(minutes=5),
    "retry_delay": timedelta(minutes=5)
}

baseURL = 'https://polizei.lu.ch'
kw = datetime.now().isocalendar()[1]
print(kw)
receivers = Variable.get("radarReceiver")
receiversList = receivers.split(',')

@dag(default_args=default_args, schedule_interval="0 18 * * 0,1,2,3,4,5,6", start_date=days_ago(2), tags=['example'])
def radar():

    @task()
    def getPdf():
        r = requests.get('https://polizei.lu.ch/organisation/sicherheit_verkehrspolizei/verkehrspolizei/spezialversorgung/verkehrssicherheit/Aktuelle_Tempomessungen')
        body = r.text
        document = Selector(text=body)
        elementsTitle = document.xpath("//a[contains(@target, '_blank')]/text()")
        title1 = elementsTitle[1].get()
        title2 = elementsTitle[2].get()
        kw1 = [int(s) for s in title1.split() if s.isdigit()][0]
        kw2 = [int(s) for s in title2.split() if s.isdigit()][0]

        print(kw1)
        print(kw2)

        elements = document.xpath("//a[contains(@target, '_blank')]")

        link1 = baseURL + elements[1].attrib['href']
        link2 = baseURL + elements[2].attrib['href']
        
        print(link1)
        print(link2)

        if not os.path.exists('test' ):
            os.makedirs('test')
            print('ordner erstellt')

        urllib.request.urlretrieve(link1, "test/kw_" + str(kw1) + ".pdf")
        urllib.request.urlretrieve(link2, "test/kw_" + str(kw2) + ".pdf")
        files1 = glob.glob("test/*.pdf")  
        print(str(files1))

    @task()
    def sendEmail():
        files = glob.glob("test/*.pdf")  
        print(files)
        content = '<h1>Radar</h1><br>' + str(kw) + '<br>' + str(files)
        send_email(
            to=receiversList[0],
            bcc=receiversList[1:],
            subject='Radar Test ' + str(kw),
            html_content=content,
            files=files
        )
    getPdf()
    sendEmail()

radar = radar()

