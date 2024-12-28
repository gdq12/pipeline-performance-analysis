import os 
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
        df: upstream output with uneeded records filtered out (with 0 passenger_count or 0 travel distance)
    """
    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Specify your transformation logic here
    print(f'Preprocessing: of {data.shape[0]}, there are {data.query("passenger_count == 0 | trip_distance == 0").shape[0]} records with no passenger count or trip distance of 0')
    # filter out unwanted rows 
    df = data.query("passenger_count > 0").query("trip_distance > 0")

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
                }).to_csv('log_script2.csv', index = False)

    print(f'exporting a df with {df.shape[0]} records')

    return df 
    
@test
def test_output(output, *args) -> None:
    """
    Determines if there was an actual output to the function 
    """
    assert output is not None, 'The output is undefined'

@test
def test_passenger_output(output, *args) -> None:
    """
    Determines if the function properly filtered out records with no passenger count 
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'

@test
def test_distance_output(output, *args) -> None:
    """
    Determined if the function properly filtered out records with no trip distance 
    """
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero travel distance'
