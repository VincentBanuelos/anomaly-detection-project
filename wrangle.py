import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn import metrics
import env
import os


def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{env.user}:{env.password}@{env.host}/{db}'

def get_logs():

    if os.path.isfile('curriculum_logs.csv'):
        return pd.read_csv('curriculum_logs.csv', index_col=0)

    else:
        ''' Pulls curriculum log from CodeUP database and translate it to dataframe'''
        query = '''
    SELECT *
    FROM logs
    LEFT JOIN cohorts ON logs.cohort_id=cohorts.id;
        '''
        
        
        df= pd.read_sql(query, get_connection('curriculum_logs'))
        df.to_csv('curriculum_logs.csv')
    return df


def prepare_log():
    ''' This prepare function set the date column as index, drop unwanted columns    and set the start date and end date to date time format'''
    df = get_logs()
    df = df.drop(columns=['deleted_at', 'updated_at', 'created_at', 'slack','cohort_id'])
    df = df.rename(columns={'path':'endpoint','name':'cohort_name'})
    # Reassign the sale_date column to be a datetime type
    df.date = pd.to_datetime(df.date)
    # Sort rows by the date and then set the index as that date
    df = df.set_index("date").sort_index()
    #set the start_date and end_date column to datetime format
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
    # data science program dataframe
    ds_df= df[(df.program_id == 3) & (df.cohort_name != 'Staff')]
    # web developers dataframe
    web_df = df[(df.program_id != 3) & (df.cohort_name != 'Staff')]
    # staff dataframe
    staff_df = df[df.cohort_name == 'Staff']
    return df,ds_df, web_df, staff_df