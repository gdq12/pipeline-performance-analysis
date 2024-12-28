import os 
import psutil
import pandas as pd
from datetime import datetime
from yellow_taxi_extract_load.utils.helpers.yellow_data_schema import schema_yellow

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

    # verify that datetime columns are in the correct format 
    datetime_cols = data.columns[data.columns.str.contains('datetime', regex = True)]

    for i in range(len(datetime_cols)):
        col_name = datetime_cols[i]
        print(f"converting {col_name} to datetime format")
        data[col_name] = pd.to_datetime(data[col_name], format = '%Y-%m-%d %H:%M:%S')

    # change the rest of the rest of the columns to other pre-assigned data types 
    df = data.astype(schema_yellow)

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
                }).to_csv('log_script4.csv', index = False)

    return df