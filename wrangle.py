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
        SELECT logs.date,  logs.time,
        logs.path as endpoint,
        logs.user_id,
        logs.ip,
        cohorts.name as cohort_name,
        cohorts.start_date,
        cohorts.end_date,
        cohorts.program_id
        FROM logs
        JOIN cohorts ON logs.cohort_id= cohorts.id;
        '''
        
        
        df= pd.read_sql(query, get_connection('curriculum_logs'))
        df.to_csv('curriculum_logs.csv')
    return df


def prepare_log():
    ''' This prepare function set the date column as index, drop unwanted columns    and set the start date and end date to date time format'''
    df = get_logs()
    #change the date column to datetime
    df.date = pd.to_datetime(df.date)
    # set date column to index
    df = df.set_index(df.date)
    #set the start_date and end_date column to datetime format
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
    #split the endpoint into 4 different sections using / as sepeartor and concatenate to the dataframe
    df= pd.concat([df, df.endpoint.str.split('/',3, expand = True)], axis=1)
    # renaming the columns created after the split of endpoint columns as page 1, page 2,page 3, page 4 respectively
    df.rename(columns={0:'page_1',1:'page_2',2:'page_3',3:'page_4'}, inplace = True)
    # data science program dataframe
    ds_df= df[(df.program_id == 3) & (df.cohort_name != 'Staff')]
    # web developers dataframe
    web_df = df[(df.program_id != 3) & (df.cohort_name != 'Staff')]
    # staff dataframe
    staff_df = df[df.cohort_name == 'Staff']
    return df,ds_df, web_df, staff_df