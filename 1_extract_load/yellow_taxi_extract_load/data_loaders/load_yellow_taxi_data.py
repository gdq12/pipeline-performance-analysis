import os 
import re 
import psutil
import pandas as pd
from datetime import datetime
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_file(*args, **kwargs):
    """
    Template for loading data from filesystem.
    Load data from 1 file or multiple file directories.

    For multiple directories, use the following:
        FileIO().load(file_directories=['dir_1', 'dir_2'])

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # year_month = kwargs.get('execution_date').date().strftime('%Y-%m')
    year_month = '2021-07'

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet'
    
    os.system(f'curl -O {url}')

    filename = f'yellow_tripdata_{year_month}.parquet'
    
    df = pd.read_parquet(filename)

    end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    pd.DataFrame({'filename': [filename],
                'log_date': kwargs.get('execution_date').date(), 
                'script_name': [kwargs.get('block_uuid')],
                'start_time': [start_time],
                'end_time': [end_time], 
                'file_size': [int(re.sub(f' {filename}', '', os.popen(f'wc -c {filename}').read()).strip())],
                'total_memory': [psutil.virtual_memory().total],
                'available_memory': [psutil.virtual_memory().available],
                'cpu_percent': [psutil.virtual_memory().percent],
                'used_memory': [psutil.virtual_memory().used],
                'free_memory': [psutil.virtual_memory().free]
                }).to_csv('log_script1.csv', index = False)
    
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
