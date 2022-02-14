from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

import time
from datetime import datetime
import tweepy
import pandas as pd
import numpy as np
import networkx as nx
import twint
import nest_asyncio
import pickle
nest_asyncio.apply()
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer

default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date':datetime(2021,12,25)
}

dag=DAG(
    'Bonus',
    default_args=default_args,
    description='Sentiment analysis tweets',
    schedule_interval='@daily')

def extract_tweets(**kwargs):
    api_key= "fpsRUPnNCiO1FdbHU8Y4sEVBY"
    api_secret_key="XHzilYWcsTZP8Mt4vKwxAO7vKH4SGBZs0APhsB4qiNXA2vGb7h"
    bearer_token="AAAAAAAAAAAAAAAAAAAAAAoFQgEAAAAAx96wwXTRKU4%2BDevXt8FH846nQ24%3DLartSvwWzsvPqehS2gJUQf4GjDbxxuiBOd8gboWiVhBtC7lwT7"
    access_token= "892369755868454912-DtFxvEWm7tEeZHY3C45PsbuE0aQRBZx"
    access_token_secret= "Xqka7CnDWStgO4CpBlUaplXfSHLmLMWAywSCQolBlGyQ4"
    auth= tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(access_token, access_token_secret)
    api=tweepy.API(auth)
    country_1="USA"
    country_2="Egypt"
    tweets_country_1=api.search_tweets(country_1, count=20)
    tweets_country_2=api.search_tweets(country_2, count=20)
    return (tweets_country_1,tweets_country_2)

def analyze_tweets(**context):
    x= context['task_instance'].xcom_pull(task_ids='extract_tweets')
    tweets_country_1=x[0]
    tweets_country_2=x[1]
    tweets_analysis_1=[]
    tweets_analysis_2=[]
    for tweet in tweets_country_1:
        sentiment=TextBlob(tweet.text, analyzer=NaiveBayesAnalyzer())
        if sentiment.sentiment.classification=='neg':
            tweets_analysis_1.append('neg')
        else:
            tweets_analysis_1.append('pos')
    for tweet in tweets_country_2:
        sentiment=TextBlob(tweet.text, analyzer=NaiveBayesAnalyzer())
        if sentiment.sentiment.classification=='neg':
            tweets_analysis_2.append('neg')
        else:
            tweets_analysis_2.append('pos')
    count=0
    avg_country_1='positive'
    avg_country_2='positive'
    for sentiment in tweets_analysis_1:
        if sentiment=='neg':
            count+=1
    if count>=10:
        avg_country_1='negative'
        
    count=0
    for sentiment in tweets_analysis_2:
        if sentiment=='neg':
            count+=1
    if count>=10:
        avg_country_2='negative'
    return (avg_country_1,avg_country_2)

def save_results(**context):
    x= context['task_instance'].xcom_pull(task_ids='analyze_tweets')
    df_olympics = pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/athlete_events.csv")
    sentiment_USA=x[0]
    sentiment_Egypt=x[1]
    df_olympics["Medal"].replace({"Gold": 3, "Silver":2, "Bronze":1, np.nan:0}, inplace=True)
    df_olympics['Medal'].unique()
    filter = df_olympics[df_olympics['Medal'] != 0]
    filterUSA=filter[filter['NOC']== 'USA']
    filterEgypt=filter[filter['NOC']=='EGY']

    UniqueMedalsUSA=filterUSA.drop_duplicates(subset=['NOC','Year','Event','Medal'])['Medal'].count()
    UniqueMedalsEgypt=filterEgypt.drop_duplicates(subset=['NOC','Year','Event','Medal'])['Medal'].count()

    s1="The average sentiment of USA which won "+str(UniqueMedalsUSA)+" medals is "+sentiment_USA
    s2="The average sentiment of Egypt which won "+str(UniqueMedalsEgypt)+" medals is "+sentiment_Egypt

    today=datetime.today()
    now = datetime.now()
    dt_string = now.strftime("%d_%m_%Y_%H_%M_%S")
    file_name=str(dt_string)+"_sentiment.txt"
    print(file_name)
    with open(file_name, 'w') as f:
        f.write(s1+", "+s2)
        f.close()
    


t1= PythonOperator(
    task_id='extract_tweets',
    provide_context=True,
    python_callable=extract_tweets,
    dag=dag
)  

t2= PythonOperator(
    task_id='analyze_tweets',
    provide_context=True,
    python_callable=analyze_tweets,
    dag=dag
)  
t3= PythonOperator(
    task_id='save_results',
    provide_context=True,
    python_callable=save_results,
    dag=dag
)  

t1>>t2>>t3