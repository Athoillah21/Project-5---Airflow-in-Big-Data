#!python3

import os
import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine


if __name__ == "__main__":
    listFile = ['bigdata_customer','bigdata_product','bigdata_transaction']
    for file in listFile:
        #read data
        path = os.getcwd()+'/data/'
        df = pd.read_csv(path + file + '.csv')
        
        #connection
        url = 'postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_db' 
        engine = create_engine(url)

        #dump data
        try:
            df.to_sql(file, index=False, con=engine, if_exists='replace')
            print(f"Data {file} Success Dump to Database")
        except:
            print(f"Data {file} Failed")