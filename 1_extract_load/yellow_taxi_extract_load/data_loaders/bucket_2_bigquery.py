import os 
import re
from google.cloud import bigquery
from yellow_taxi_extract_load.utils.helpers.dict_helpers import sql_dict_yellow
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
    # cloud storage centric vars 
    bucket_name = kwargs.get('bucket_name_data')
    table_name = re.sub('.parquet', '', os.popen('ls *.parquet').read()).strip()
    root_path = f"{bucket_name}/{kwargs.get('execution_date').date()}_{table_name}"

    # query centric vars 
    db_name = kwargs.get('gcp_project_name')
    tbl_name_substr = kwargs.get('table_name') 
    col_param = ' '.join([key + ' ' + item + ',' for key, item in sql_dict_yellow.items()])[:-1]
    col_names = ' '.join([key + ',' for key in sql_dict_yellow.keys()])[:-1]

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
    select * from `{db_name}.nytaxi_raw.external_{tbl_name_substr}`
    """

    # get BigQuery connection 
    client = bigquery.Client()

    # execute queries 
    print('creating if not already present schemas')
    client.query(q1a)
    client.query(q1b)
    client.query(q1c)
    client.query(q1d)

    print('creating external table')
    client.query(q2)

    print('populating stage table')
    client.query(q3a)
    client.query(q3b)

    print('loading data to stage complete')