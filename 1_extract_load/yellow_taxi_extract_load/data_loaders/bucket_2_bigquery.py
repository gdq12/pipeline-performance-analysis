from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from os import path
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

    q1 = f"""create schema if not exists `{kwargs.get('gcp_project_name')}`.`nytaxi_raw`
    options (location = 'EU')
    """

    q2 = f"""create schema if not exists `{kwargs.get('gcp_project_name')}`.`nytaxi_transform`
    options (location = 'EU')
    """

    q3 = f"""create schema if not exists `{kwargs.get('gcp_project_name')}`.`nytaxi_prod`
    options (location = 'EU')
    """

    q4 = f"""create or replace external table {kwargs.get('gcp_project_name')}.nytaxi_raw.external_{kwargs.get('table_name')}
    options (
    format = 'PARQUET',
    uris = ['gs://{kwargs.get('bucket_name_data')}/{kwargs.get('execution_date').date()}_yellow_taxi_data/*']
    )
    """

    q5 = f"""create if not exists {kwargs.get('gcp_project_name')}.nytaxi_raw.{kwargs.get('table_name')} 
    (vendor_id string, passenger_count int64, trip_distance float64, ratecode_id string, store_and_fwd_flag string, pu_location_id string, do_location_id string, payment_type string, fare_amount float64, extra float64, mta_tax float64, tip_amount float64, tolls_amount float64, improvement_surcharge float64, total_amount float64, congestion_surcharge float64, airport_fee float64)
    """

    q6 = f"""insert into {kwargs.get('gcp_project_name')}.nytaxi_raw.{kwargs.get('table_name')} 
    select * from {kwargs.get('gcp_project_name')}.nytaxi_raw.external_{kwargs.get('table_name')}
    """

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q1)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q2)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q3)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q4)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q5)
    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(q6)

    # return BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).load(query)
