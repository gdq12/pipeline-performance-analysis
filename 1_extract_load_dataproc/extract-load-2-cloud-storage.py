import os
import argparse
import time
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
# for local runs 
# from helper_funcs import load_trip_data_local, dimension_name_cleanup, data_2_gcp_cloud_storage_local, bucket_2_ext_tbl, bucket_2_bigquery
# for dataproc runs 
from helper_funcs import load_trip_data_dataproc, dimension_name_cleanup, data_2_gcp_cloud_storage_dataproc, bucket_2_ext_tbl, bucket_2_bigquery
# additional queries needed 
from dict_query_helpers import q_history
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import logging

# for logging info 
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logging.info('parsing input arguments')
parser = argparse.ArgumentParser()

parser.add_argument('--gcp_id')
parser.add_argument('--gcp_file_cred') #only for dataproc run
parser.add_argument('--trip_name')
parser.add_argument('--start_date')
parser.add_argument('--end_date')

args = parser.parse_args()

# resource subseting
trip_subset = args.trip_name #'yellow'

# date ranges
start_dt = datetime.strptime(args.start_date,'%Y-%m-%d')#datetime.strptime('2009-01-01','%Y-%m-%d')
end_dt = datetime.strptime(args.end_date,'%Y-%m-%d')#datetime.strptime('2009-03-01','%Y-%m-%d')
delta = relativedelta(months=1)

# var that stays the same through out run
parq_subset= f'{trip_subset}_tripdata'
ext_table_name = f'external_{parq_subset}'
schema_raw = 'nytaxi_raw'
schema_stage = 'nytaxi_stage'
time_subset = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
q_history_time = time_subset.replace('_', ' ')
bucket_url = f'gs://original-parquet-url'
bucket_data = f'{trip_subset}-taxi-data-extract-load'
project_id = args.gcp_id#os.getenv('GCP_ID')

logging.info(f"will be fetching parquest for {parq_subset}, from {start_dt} - {end_dt}")

#########################################local#############################################################################
# os.system('gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS')
###########################################################################################################################

# connecting to bigquery 
# credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')) #local
credentials = service_account.Credentials.from_service_account_file(args.gcp_file_cred) #dataproc
client = bigquery.Client(credentials = credentials, project = project_id)

# start spark session 
logging.info('starting spark session')

spark = SparkSession.builder \
    .master("local[4]") \
    .appName('extract-load-spark') \
    .getOrCreate()

while start_dt <= end_dt:

    # vars for fetching parquet
    year_month = start_dt.strftime("%Y-%m")
    filename = f"{parq_subset}_{year_month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    
    logging.info(f'starting iteration for {parq_subset}, {year_month}')
    
    # vars for gcp/bucket
    root_path = f"gs://{bucket_data}/{time_subset}_{parq_subset}_{year_month}"
        
    ###############################################local run###################################################################
    # df = load_trip_data_local(spark, url, filename)
    ###############################################data proc run###############################################################
    df = load_trip_data_dataproc(spark, url, filename, bucket_url)
    ###########################################################################################################################

    # fix parquets a bit before the push 
    df = dimension_name_cleanup(df)
    
    # push parquets to bucket
    #################################################local run#################################################################
    # data_2_gcp_cloud_storage_local(df, parq_subset, year_month, root_path, filename)
    ############################################dataproc run###################################################################
    data_2_gcp_cloud_storage_dataproc(df, root_path)
    ###########################################################################################################################
    
    # stageing data load
    bucket_2_ext_tbl(project_id, ext_table_name, root_path, client)

    # copying data to a stage schema 
    bucket_2_bigquery(client, project_id, ext_table_name, schema_raw, schema_stage, parq_subset, filename)
    
    #save info in qhistory table 
    client.query(q_history.format(project_id, root_path, q_history_time))
        
    start_dt += delta

# close spark session
spark.stop()