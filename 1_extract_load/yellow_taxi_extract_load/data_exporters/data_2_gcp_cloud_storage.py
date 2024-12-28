import os 
import re 
import psutil
from datetime import datetime
import pandas as pd
import pyarrow as pa 
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):

    start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # get the needed GCP cloud storage vars 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs.get('GOOGLE_APPLICATION_CREDENTIALS')

    bucket_name = kwargs.get('bucket_name_data')

    table_name = kwargs.get('table_name')
    
    # partition the data 
    pickup_col = data.columns[data.columns.str.contains('pickup', regex = True)][0]
    col_name = re.sub('time', '', pickup_col)
    data[col_name] = data[pickup_col].dt.date

    print(f"will be pushing the data into GCP with {data[col_name].nunique()} partitions")

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    root_path = f"{bucket_name}/{kwargs.get('execution_date').date()}_{table_name}"

    print(f'loading parquets to {root_path}')

    pq.write_to_dataset(
        table, 
        root_path = root_path,
        partition_cols = [col_name],
        filesystem = gcs
    )

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
                }).to_csv('log_script5.csv', index = False)