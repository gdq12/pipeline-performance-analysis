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

    kwarg_logger = kwargs.get('logger')

    # get the needed GCP cloud storage vars 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = kwargs.get('GOOGLE_APPLICATION_CREDENTIALS')

    bucket_name = kwargs.get('bucket_name_data')

    table_name = re.sub('.parquet', '', os.popen('ls *.parquet').read()).strip()

    # create new column for partitioning 
    pickup_col = data.columns[data.columns.str.contains('pickup', regex = True)][0]
    col_name = re.sub('time', '', pickup_col)
    data[col_name] = data[pickup_col].dt.date

    kwarg_logger.info(f"will be pushing the data into GCP with {data[col_name].nunique()} partitions")

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    root_path = f"{bucket_name}/{datetime.now().strftime('%Y-%m-%d')}_{table_name}"

    kwarg_logger.info(f'loading parquets to {root_path}')

    pq.write_to_dataset(
        table, 
        root_path = root_path,
        partition_cols = [col_name],
        filesystem = gcs
    )

    kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")