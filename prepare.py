import pandas as pd
import numpy as np
from acquire import get_tsa, opsd_data


#==============Data_prep_exercise functions==========



def prep_tsa():
    '''
    gets and prepares the tsa_item_demand data
    '''
    df = get_tsa()
    
    df.sale_date = pd.to_datetime(df.sale_date)
    
    df = df.set_index('sale_date')
    
    df = df.sort_values('sale_date')
    
    df['month'] = df.index.month_name()
    
    df['day_of_week'] = df.index.day_name()

    df['sales_total'] = df.sale_amount * df.item_price

    return df


def prep_opsd():
    '''
    gets and prepares opsd_germany_daily data
    '''
    df = opsd_data()
    
    df.columns = [col.lower().replace('+', '_') for col in df.columns]
    
    df.date = pd.to_datetime(df.date)
    
    df = df.set_index('date')
    
    df = df.sort_values('date')
    
    df['month'] = df.index.strftime('%B')
    
    df['year'] = df.index.year
        
    df = df.fillna(0)

    return df

