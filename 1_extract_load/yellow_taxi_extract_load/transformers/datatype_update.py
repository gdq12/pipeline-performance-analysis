import os 
import re
import psutil
import pandas as pd
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        df: pandas df with the data types that are specified in utils.helper files
    """
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # create new column for partitioning in next step 
    pickup_col = data.columns[data.columns.str.contains('pickup', regex = True)][0]
    col_name = re.sub('time', '', pickup_col)
    data[col_name] = data[pickup_col].dt.date
    
    # convert everything to string for better loading to BigQuery tables 
    df = data.astype(str)

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    pd.DataFrame({'filename': [''],
                'log_date': kwargs.get('execution_date').date(),
                'script_name': [kwargs.get('block_uuid')],
                'start_time': [start_time],
                'end_time': [end_time], 
                'file_size': [''],
                'total_memory': [psutil.virtual_memory().total],
                'available_memory': [psutil.virtual_memory().available],
                'cpu_percent': [psutil.virtual_memory().percent],
                'used_memory': [psutil.virtual_memory().used],
                'free_memory': [psutil.virtual_memory().free]
                }).to_csv('log_script3.csv', index = False)

    return df