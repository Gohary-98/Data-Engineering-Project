from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator



from datetime import datetime

import pandas as pd
import numpy as np
import json
from scipy import stats
from scipy.stats import pearsonr
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past':False,
    'start_date':datetime(2021,12,28)
}
dag=DAG(
    'Milestone3',
    default_args=default_args,
    description='Fetch our olympics dataset',
    schedule_interval='@once')
    
def extract_data(**kwargs):
    df_olympics = pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/athlete_events.csv")
    
    return df_olympics
    
def clean_data(**context):
    df_olympics= context['task_instance'].xcom_pull(task_ids='extract_data')
    df_olympics["Age"] = df_olympics["Age"].astype(float)
    df_olympics["Height"] = df_olympics["Height"].astype(float)
    df_olympics["Weight"] = df_olympics["Weight"].astype(float)
    
    Gender_Sport=df_olympics.groupby(["Sex","Sport"])
    keys = Gender_Sport.groups.keys()
    for gender,sport in keys:
        df_players_gender_sport=df_olympics[ (df_olympics["Sex"]==gender) & (df_olympics["Sport"]==sport)].drop_duplicates(subset=['ID'])
        df_players_gender_sport = df_players_gender_sport.dropna(subset=['Age'])
        z_age=np.abs(stats.zscore(df_players_gender_sport['Age']))
        df_not_outliers = df_players_gender_sport[(z_age < 2)]
        outliers=(z_age >= 2)
        filtered_entries = z_age < 2
        df_zscore_filter = df_players_gender_sport[filtered_entries]
        meanAge=df_not_outliers["Age"].mean()
        df_players_gender_sport.loc[outliers,"Age"]=meanAge
        inside = df_players_gender_sport[df_players_gender_sport["Age"] == meanAge]
        df_olympics.loc[df_olympics.ID.isin(inside.ID),"Age"] = meanAge
    Sex_Sport_Age = df_olympics.groupby(["Sex","Sport"])["Age"].mean()
    df_olympics.loc[df_olympics['Sex'] == "F", "Age"] = df_olympics['Age'].fillna(df_olympics.loc[df_olympics['Sex'] == "F"]["Sport"].map(Sex_Sport_Age[('F',)]))
    df_olympics.loc[df_olympics['Sex'] == "M", "Age"] = df_olympics['Age'].fillna(df_olympics.loc[df_olympics['Sex'] == "M"]["Sport"].map(Sex_Sport_Age[('M',)]))
    
    
    
    Gender_Sport = df_olympics.dropna(axis=0,subset=['Weight']).groupby(["Sex","Sport"])
    keys = Gender_Sport.groups.keys()
    for gender,sport in keys:
        df_players_gender_sport=df_olympics[ (df_olympics["Sex"]==gender) & (df_olympics["Sport"]==sport)].drop_duplicates(subset=['ID'])
        df_players_gender_sport = df_players_gender_sport.dropna(subset=['Weight'])
        z_weight=np.abs(stats.zscore(df_players_gender_sport['Weight']))
        df_not_outliers = df_players_gender_sport[(z_weight < 2)]
        outliers=(z_weight >= 2)
        filtered_entries = z_weight <= 2
        df_zscore_filter = df_players_gender_sport[filtered_entries]
        meanWeight=df_not_outliers["Weight"].mean()
        df_players_gender_sport.loc[outliers,"Weight"]=meanWeight
        inside = df_players_gender_sport[df_players_gender_sport["Weight"] == meanWeight]
        df_olympics.loc[df_olympics.ID.isin(inside.ID),"Weight"] = meanWeight
    Sex_Sport_Weight = df_olympics.groupby(["Sex","Sport"])["Weight"].mean()
    df_olympics.loc[df_olympics['Sex'] == "F", "Weight"] = df_olympics['Weight'].fillna(df_olympics.loc[df_olympics['Sex'] == "F"]["Sport"].map(Sex_Sport_Weight[('F',)]))
    df_olympics.loc[df_olympics['Sex'] == "M", "Weight"] = df_olympics['Weight'].fillna(df_olympics.loc[df_olympics['Sex'] == "M"]["Sport"].map(Sex_Sport_Weight[('M',)]))
    meanWeightMale=df_olympics.loc[df_olympics['Sex'] == "M"]["Weight"].mean()
    meanWeightFemale=df_olympics.loc[df_olympics['Sex'] == "F"]["Weight"].mean()
    df_olympics["Weight"]=df_olympics['Weight'].fillna(df_olympics['Sex'].map({'F':meanWeightFemale, 'M': meanWeightMale}))
    
    Gender_Sport = df_olympics.dropna(subset=['Height']).groupby(["Sex","Sport"])
    keys = Gender_Sport.groups.keys()
    for gender,sport in keys:
        df_players_gender_sport=df_olympics[ (df_olympics["Sex"]==gender) & (df_olympics["Sport"]==sport)].drop_duplicates(subset=['ID'])
        df_players_gender_sport = df_players_gender_sport.dropna(subset=['Height'])
        z_height=np.abs(stats.zscore(df_players_gender_sport['Height']))
        df_not_outliers = df_players_gender_sport[(z_height < 2)]
        outliers=(z_height >= 2)
        filtered_entries = z_height <= 2
        df_zscore_filter = df_players_gender_sport[filtered_entries]
        meanHeight=df_not_outliers["Height"].mean()
        df_players_gender_sport.loc[outliers,"Height"]=meanHeight
        inside = df_players_gender_sport[df_players_gender_sport["Height"] == meanHeight]
        df_olympics.loc[df_olympics.ID.isin(inside.ID),"Height"] = meanHeight
    Sex_Sport_Height = df_olympics.groupby(["Sex","Sport"])["Height"].mean()
    df_olympics.loc[df_olympics['Sex'] == "F", "Height"] = df_olympics['Height'].fillna(df_olympics.loc[df_olympics['Sex'] == "F"]["Sport"].map(Sex_Sport_Height[('F',)]))
    df_olympics.loc[df_olympics['Sex'] == "M", "Height"] = df_olympics['Height'].fillna(df_olympics.loc[df_olympics['Sex'] == "M"]["Sport"].map(Sex_Sport_Height[('M',)]))
    meanHeightMale=df_olympics.loc[df_olympics['Sex'] == "M"]["Height"].mean()
    meanHeightFemale=df_olympics.loc[df_olympics['Sex'] == "F"]["Height"].mean()
    df_olympics["Height"]=df_olympics['Height'].fillna(df_olympics['Sex'].map({'F':meanHeightFemale, 'M': meanHeightMale}))
    df_olympics["Medal"].replace({"Gold": 3, "Silver":2, "Bronze":1, np.nan:0}, inplace=True)
    
    return df_olympics
    
def integrate_data(**context):
    df_olympics= context['task_instance'].xcom_pull(task_ids='clean_data')
    df_hosting_continents=pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/Hosting_Continent.csv")
    df_continents=pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/continents.csv")
    df_hosting_country=pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/Hosting_Country.csv")
    noc_regions=pd.read_csv("https://raw.githubusercontent.com/Gohary-98/datasets_test/main/noc_regions.csv")
    df_olympics_v2 = pd.merge(df_olympics, df_hosting_country,how='left')
    df_olympics_v3 = pd.merge(df_olympics_v2, noc_regions,on='NOC',how='left')
    df_olympics_v3.loc[df_olympics_v3.NOC == "SGP", 'region'] = "Singapore"
    df_olympics_v4 = df_olympics_v3.drop('notes', 1)
    df_olympics_v4["region"].fillna("Other",inplace=True)
    df_olympics_v5= pd.merge(df_olympics_v4, df_continents,on=['NOC','region'],how='left')
    df_olympics_v5.loc[df_olympics_v5.NOC == "SGP", 'Continent'] = "Asia"
    df_olympics_v5["Continent"].fillna("Other",inplace=True)
    df_olympics_new = pd.merge(df_olympics_v5, df_hosting_continents,on='City',how='left')
    
    return df_olympics_new
    
def feature_data(**context):
    df_olympics_new= context['task_instance'].xcom_pull(task_ids='integrate_data')
    df_olympics_new['BMI'] = (df_olympics_new['Weight'])/((df_olympics_new['Height']/100)**2)
    BMI_Group = [] # define array structure
    for bmi in df_olympics_new["BMI"]:
        if bmi < 16:
            BMI_Group.append(0)
        elif 16 <= bmi < 18:
            BMI_Group.append(1)
        elif 18 <= bmi < 25:
            BMI_Group.append(2)
        elif 25 <= bmi < 30:
            BMI_Group.append(1)
        elif 30 <= bmi:
            BMI_Group.append(0)
        else:
            BMI_Group.append(0)
            
    df_olympics_new["BMI Group"] = BMI_Group
    
    temp=df_olympics_new.drop_duplicates(subset=['ID','Year']).sort_values(by=['Year'])
    x=temp.groupby(['ID'])
    occ=temp.groupby(['ID']).cumcount()
    temp['Participation Number']=occ+1
    id_year_occ=temp[['ID','Year','Participation Number']]
    df_olympics_final=pd.merge(df_olympics_new,id_year_occ,how='left')
    df_olympics_final=df_olympics_final.sort_values('ID')
    
    return df_olympics_final
    
    
def store_data(**context):
    df_olympics_final= context['task_instance'].xcom_pull(task_ids='feature_data')
    print(df_olympics_final.shape)
    print(df_olympics_final.isnull().sum())
    print(df_olympics_final.info())
    print(df_olympics_final.describe())
    print(os.path.abspath(os.getcwd()))
    df_olympics_final.to_csv("df_olympics_out.csv")
    
t1= PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag
)  

t2= PythonOperator(
    task_id='clean_data',
    provide_context=True,
    python_callable=clean_data,
    dag=dag
)  

t3= PythonOperator(
    task_id='integrate_data',
    provide_context=True,
    python_callable=integrate_data,
    dag=dag
)

t4= PythonOperator(
    task_id='feature_data',
    provide_context=True,
    python_callable=feature_data,
    dag=dag
)

t5= PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=store_data,
    dag=dag
)

t1>>t2>>t3>>t4>>t5