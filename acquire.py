from env import get_connection
import os
import pandas as pd
import numpy as np
import urllib.request
import requests


#==============API_acquition_exercise function==========

def get_swapi_data(resource):
    """
    Retrieve data from the Star Wars API (SWAPI) for a specified resource.

    Args:
        resource (str): The name of the SWAPI resource (e.g., 'people', 'planets', 'starships').

    Returns:
        pd.DataFrame: A DataFrame containing data from the specified resource.
    """
    url = f'https://swapi.dev/api/{resource}/'
    data = []

    while url:
        response = requests.get(url)
        if response.status_code == 200:
            page_data = response.json()
            data.extend(page_data['results'])
            url = page_data['next']
        else:
            raise Exception(f"Failed to retrieve data for {resource} from SWAPI.")
    
    return pd.DataFrame(data)

def save_data_to_csv(df, filename):
    """
    Save a DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The DataFrame to be saved.
        filename (str): The name of the CSV file (including extension) to save the DataFrame to.
    """
    df.to_csv(filename, index=False)

def acquire_star_wars_data():
    """
    Acquire data for people, planets, and starships from SWAPI and save to CSV files.
    """
    people = get_swapi_data('people')
    planets = get_swapi_data('planets')
    starships = get_swapi_data('starships')

    save_data_to_csv(people, 'people.csv')
    save_data_to_csv(planets, 'planets.csv')
    save_data_to_csv(starships, 'starships.csv')

def combine_star_wars_data():
    """
    Combine data from CSV files into one large DataFrame and save to a new CSV file.
    """
    df1 = pd.read_csv('people.csv')
    df2 = pd.read_csv('planets.csv')
    df3 = pd.read_csv('starships.csv')

    combined_df = pd.concat([df1, df2, df3])
    combined_df.to_csv('starwars.csv', index=False)


if __name__ == "__main__":
    # Run the data acquisition functions
    acquire_star_wars_data()
    combine_star_wars_data()
    print("Data acquisition and processing complete.")



#==============DateTime Acquire exercise functions==========


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

def get_tsa():
    '''
    a function that searches for and retieves data. 
    if it does not find it it will retirve the data 
    from os it will query database and pull from
    url
    args: 
    return:
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
    ar
    '''
    url = 'https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv'
    
    urllib.request.urlretrieve(url, 'opsd_germany_daily.csv')
    
    df = pd.read_csv('opsd_germany_daily.csv')

    return df
        