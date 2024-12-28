import os 
import re
import pandas as pd 
from google.cloud import storage
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@custom
def log_upload_and_cleanup_env(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # fetch logs saved from csv files 
    print('collating all the logs files of the run')
    filenames = [name for name in os.listdir() if '.csv' in name]

    df_list = []
    for i in range(len(filenames)):
        filename = filenames[i]
        dd = pd.read_csv(filename)
        df_list.append(dd)
    
    df = pd.concat(df_list, axis = 0).sort_values(['filename', 'file_size']).ffill()

    df.to_csv('log_file_4_cloud.csv', index = False)

    # push the data to Cloud Storage 
    print('pushing the collated logs to Cloud Storage')

    parquet_filename = re.sub('.parquet', '', os.popen('ls *.parquet').read()).strip()

    bucket_name = kwargs.get('bucket_name_log')

    root_path = f"{kwargs.get('execution_date').date()}_parquet_{parquet_filename}.csv"

    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(root_path)
    blob.upload_from_filename('log_file_4_cloud.csv')

    # cleanup the env for the next run 
    os.system('rm -r *.parquet')

    os.system('rm -r *.csv')

    print('parquet and csv files removed')