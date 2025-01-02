import os 
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
    kwarg_logger = kwargs.get('logger')

    if 'yellow' not in kwargs['pipeline_uuid']:
        exec_date = datetime.strptime(kwargs['execution_date_trigger'], "%Y-%m-%dT%H:%M:%S")
    else:
        exec_date = kwargs.get('execution_date')

    year_month = exec_date.date().strftime('%Y-%m')
    # year_month = '2019-10'

    filename = f"{kwargs.get('table_name')}_{year_month}.parquet"

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

    kwarg_logger.info(f"fetching {filename} from {url}")
    
    os.system(f'curl -O {url}') 

    try:
        df = pd.read_parquet(filename)
        kwarg_logger.info(f"fetched {filename} with dimensions {df.shape}")
        kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")
        return df#.iloc[:100000]
    except Exception as e:
        os.system(f"rm -r {kwargs.get('table_name')}*.parquet")
        kwarg_logger.info(f"there was an error with the {filename}: {e}, ABORTED PIPELINE!!!!")
        kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")
        return f'there was an error with the {filename}: {e}, ABORTED PIPELINE!!!!'