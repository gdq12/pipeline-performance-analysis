import os 
import re
import psutil
import time 
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from extract_load.utils.helpers.dict_helpers import schema_yellow_dict, schema_green_dict, schema_fhv_dict, schema_fhvhv_dict
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_big_query(*args, **kwargs):
    """
    Template for loading data from a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    kwarg_logger = kwargs.get('logger')

    # connecting to bigquery 
    credentials = service_account.Credentials.from_service_account_file(kwargs.get('GOOGLE_APPLICATION_CREDENTIALS'))
    project_id = kwargs.get('gcp_project_name')

    # cloud storage centric vars 
    bucket_name = kwargs.get('bucket_name_data')
    table_name = re.sub('.parquet', '', os.popen(f"ls {kwargs.get('table_name')}*.parquet").read()).strip()
    root_path = f"{bucket_name}/{datetime.now().strftime('%Y-%m-%d')}_{table_name}"

    # query centric vars 
    db_name = kwargs.get('gcp_project_name')
    tbl_name_substr = kwargs.get('table_name') 
    
    if 'yellow' in tbl_name_substr:
        col_param = ' '.join([key + ' ' + item + ',' for key, item in schema_yellow_dict.items()])[:-1]
        col_names = ' '.join([key + ',' for key in schema_yellow_dict.keys()])[:-1]
    elif 'green' in tbl_name_substr:
        col_param = ' '.join([key + ' ' + item + ',' for key, item in schema_green_dict.items()])[:-1]
        col_names = ' '.join([key + ',' for key in schema_green_dict.keys()])[:-1]
    elif 'fhv' in tbl_name_substr:
        col_param = ' '.join([key + ' ' + item + ',' for key, item in schema_fhv_dict.items()])[:-1]
        col_names = ' '.join([key + ',' for key in schema_fhv_dict.keys()])[:-1]
    elif 'fhvhv' in tbl_name_substr:
        col_param = ' '.join([key + ' ' + item + ',' for key, item in schema_fhvhv_dict.items()])[:-1]
        col_names = ' '.join([key + ',' for key in schema_fhvhv_dict.keys()])[:-1]
    else:
        pass

    q1a = f"""create schema if not exists `{db_name}`.`nytaxi_raw`
    options (location = 'EU')
    """

    q1b = f"""create schema if not exists `{db_name}`.`nytaxi_stage`
    options (location = 'EU')
    """

    q1c = f"""create schema if not exists `{db_name}`.`nytaxi_transform`
    options (location = 'EU')
    """

    q1d = f"""create schema if not exists `{db_name}`.`nytaxi_prod`
    options (location = 'EU')
    """

    q2 = f"""create or replace external table `{db_name}`.`nytaxi_raw.external_{tbl_name_substr}`
    options (
    format = 'PARQUET',
    uris = ['gs://{root_path}/*']
    )
    """

    q3a = f"""create table if not exists `{db_name}`.`nytaxi_stage`.`{tbl_name_substr}`
    ({col_param})
    """

    q3b = f"""insert into `{db_name}`.`nytaxi_stage`.`{tbl_name_substr}`
    ({col_names})
    select *, current_timestamp() from `{db_name}.nytaxi_raw.external_{tbl_name_substr}`
    """

    # get BigQuery connection 
    client = bigquery.Client(credentials = credentials, project = project_id)

    # execute queries 
    kwarg_logger.info('creating if not already present schemas')
    client.query(q1a)
    client.query(q1b)
    client.query(q1c)
    client.query(q1d)

    kwarg_logger.info('creating external table')
    time.sleep(10)
    client.query(q2)

    kwarg_logger.info('populating stage table')
    time.sleep(10)
    client.query(q3a)
    time.sleep(10)
    client.query(q3b)

    kwarg_logger.info('loading data to stage complete')

    # cleanup env for next run 
    os.system('rm -r *.parquet')

    kwarg_logger.info('cleanuped up env for next run')

    kwarg_logger.info(f"env memory stats: {psutil.virtual_memory().total} (total memory), {psutil.virtual_memory().available} (available), {psutil.virtual_memory().percent} (percent), {psutil.virtual_memory().used} (used), {psutil.virtual_memory().free} (free)")
