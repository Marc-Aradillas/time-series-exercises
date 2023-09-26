from env import get_connection
import os
import pandas as pd
import numpy as np
import urllib.request
import requests


url = get_connection('tsa_item_demand')

query = '''
        SELECT sale_date, sale_amount, 
        item_brand, item_name, item_price, 
        store_address, store_zipcode, 
        store_city, store_state
        FROM sales
        LEFT JOIN items USING(item_id)
        LEFT JOIN stores USING(store_id)
        '''

def get_data():
    '''
    a function that searches for and retieves data. 
    if it does not find it it will retirve the data 
    from os it will query database and pull from
    url
    '''

    filename = 'tsa_item_data.csv'

    if os.path.isfile(filename):

        return pd.read_csv(filename)
        
    else:
        
        df = pd.read_sql(query, url)
        df.to_csv(filename, index=0)

        return df




def opsd_data():
    '''
    gets opsd germany data
    '''
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    
    urllib.request.urlretrieve(url, 'opsd_germany_daily.csv')
    
    df = pd.read_csv('opsd_germany_daily.csv')

    return df
        