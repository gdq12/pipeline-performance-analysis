# needed libararies 
import os
import re
import argparse
import logging 
import time
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from dict_query_helpers import schema_yellow_dict2009, schema_yellow_dict2010, schema_yellow_dict, schema_green_dict, schema_fhv_dict, schema_fhvhv_dict, schema_log_dict
from dict_query_helpers import q_log_skeleton, q_history
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from py4j.protocol import Py4JJavaError

def parquet_downloaded(data):
    if type(data) != str:
        return True
    else:
        return False 
        
def parquet_not_downloaded(data):
    if data == None:
        return True
    else: 
        return False

def abort_pipeline():
    print("faulty parquet file downloaded, iteration run aborted")

##########################################local############################################################################
# def load_trip_data(spark, url, filename):
    
#     # down load parquet
#     print(f'fetching {filename} from {url}')
#     os.system(f'curl -O {url}') 
    
#     try:
#         # read parquet into environment 
#         df = spark.read \
#             .option("header", "true") \
#             .parquet(filename)
#         print(f'loaded {filename} in spark session with dimensions ({df.count()}, {len(df.columns)})')
#         return df
#     except Py4JJavaError as e:
#         os.system(f"rm -r {filename}")
#         print(f"{filename} not a valid parquet file, aborted pipeline!!!!") 
#         return None
##########################################dataproc#########################################################################
def load_trip_data(spark, url, filename, bucket_url):
    
    # down load parquet
    print(f'fetching {filename} from {url}')
    # os.system(f'curl -O {url}') 
    # os.system(f'gsutil -m cp {filename} {bucket_url}')
    
    try:
        # read parquet into environment 
        df = spark.read \
            .option("header", "true") \
            .parquet(f"{bucket_url}/{filename}")
        print(f'loaded {bucket_url}/{filename} in spark session ')
        return df
    except Py4JJavaError as e:
        print(f"aborted pipeline for {bucket_url}/{filename} due to error")
        return None
###########################################################################################################################

def dimension_name_cleanup(df):
    print(f"Spark DF currently has the following columns: {', '.join(df.columns)}")
    # cleanup column names 
    for i in range(len(df.columns)):
        col_name = df.columns[i]
        col_name_new = re.sub('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', '_', col_name).lower()
        df = df.withColumnRenamed(col_name, col_name_new) 
    print(f"Spark DF has been updated with the following columns: {', '.join(df.columns)}")
    return df
    
##########################################local############################################################################
# def data_2_gcp_cloud_storage(df, table_name, year_month, root_path, filename):
#     # repartition df for export 
#     pickup_col = [col for col in df.columns if 'pickup' in col][0]
#     col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
#     df = df.alias('a') \
#             .select('a.*', \
#                     F.date_trunc('day', f'a.{pickup_col}') \
#                     .alias(col_name))
    
#     print(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

#     # repartition and save locally 
#     os.system(f'mkdir {table_name}_{year_month}')

#     print(f"saving partitions in {table_name}_{year_month}")
    
#     df.repartition(col_name) \
#         .write.parquet(f'{table_name}_{year_month}', mode = 'overwrite') 

#     # authenticate connex to cloud storage
#     os.system('gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS')

#     print(f"loading parquets to {root_path}")
    
#     # copy parquets to cloud storage
#     os.system(f'gsutil -m cp -r {table_name}_{year_month}/*.parquet {root_path}')
    
#     print('cleaning up environment for next run')
#     # remove parquets locally to make sure pull from gcp worked
#     os.system(f'rm -r {table_name}_{year_month}')
#     os.system(f'rm {filename}')
    
##########################################dataproc#########################################################################
def data_2_gcp_cloud_storage(df, root_path):
    # repartition df for export 
    pickup_col = [col for col in df.columns if 'pickup' in col][0]
    col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
    df = df.alias('a') \
            .select('a.*', \
                    F.date_trunc('day', f'a.{pickup_col}') \
                    .alias(col_name))
    
    print(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

    # repartition and save locally 
    os.system(f'gcloud storage folders create {root_path}')

    print(f"loading parquets to {root_path}")
    
    df.repartition(col_name) \
        .write.parquet(root_path, mode = 'overwrite')

    print(f'removing un-needed _SUCCESS file from folder')
    os.system(f'gsutil rm -r {root_path}/_SUCCESS')
    
    print(f"loading partitions to {root_path} complete")

    print('cleaning up environment for next run')
    
###########################################################################################################################

def bucket_2_ext_tbl(project_id, table_name, root_path, client):
    
    q1a = f"""create schema if not exists `{project_id}`.`nytaxi_raw`
    options (location = 'EU')
    """

    q1b = f"""create schema if not exists `{project_id}`.`nytaxi_stage`
    options (location = 'EU')
    """

    q1c = f"""create schema if not exists `{project_id}`.`nytaxi_transform`
    options (location = 'EU')
    """

    q1d = f"""create schema if not exists `{project_id}`.`nytaxi_prod`
    options (location = 'EU')
    """

    q2 = f"""create or replace external table `{project_id}`.`nytaxi_raw.external_{table_name}`
    options (
    format = 'PARQUET',
    uris = ['{root_path}/*']
    )
    """
    
    # execute queries 
    print('creating if not already present schemas')
    client.query(q1a)
    client.query(q1b)
    client.query(q1c)
    client.query(q1d)

    print('creating external table')
    time.sleep(10)
    client.query(q2)

    print('loading external table complete')

def update_log_tbl(schema_log_dict, data_schema_dict, schema, where_substr, table_name, client, q_log_skeleton, project_id, where1, where2):

    col_param = ' '.join([key + ' ' + item + ',' for key, item in schema_log_dict.items()])[:-1]
    col_names = ' '.join([key + ',' for key in schema_log_dict.keys()])[:-1]

    q1 = f"""create table if not exists `{project_id}`.`nytaxi_raw`.`extract_load_log`
    ({col_param})
    """

    print('logging extract - load')
    client.query(q1)

    # columns that cant calc avg on
    str_cols = ['time', 'date', 
                'vendor_name', 'vendor_id', 
                'store_and_fwd_flag', 
                'dispatching_base_num', 'affiliated_base_number',
                'hvfhs_license_num', 'dispatching_base_num', 'originating_base_num',
                'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag', 'wav_request_flag', 'wav_match_flag']

    for key in data_schema_dict.keys():
        print(f'fetching stats for column {key}')
        tbl_schema = f"`{project_id}`.`{schema}`"
        col_name = key
        if key in ('creation_dt', 'data_source'):
            print(f"no need to collect stats on column, moving on")
            pass
        elif re.findall("|".join(str_cols), key):
            mean_col = 'null' 
            q_log = q_log_skeleton.format(project_id, col_names,
                                        tbl_schema, where_substr, 
                                         col_name, col_name, project_id, schema, table_name, where1, col_name, 
                                         root_path, project_id, schema, table_name,
                                         col_name, col_name, col_name, col_name, mean_col, col_name, 
                                         project_id, schema, table_name, col_name,
                                         where2)
        else:
            mean_col = f'avg(tbl.{key})'
            q_log = q_log_skeleton.format(project_id, col_names,
                                        tbl_schema, where_substr, 
                                         col_name, col_name, project_id, schema, table_name, where1,col_name,
                                         root_path, project_id, schema, table_name,
                                         col_name, col_name, col_name, col_name, mean_col, col_name, 
                                         project_id, schema, table_name, col_name,
                                         where2)
        client.query(q_log)

    print('loading log table for extrnal tables complete')
    
def bucket_2_bigquery(data_schema_dict, project_id, table_name, root_path, client):
    
    col_param = ' '.join([key + ' ' + item + ',' for key, item in data_schema_dict.items()])[:-1]
    col_names = ' '.join([key + ',' for key in data_schema_dict.keys()])[:-1]

    q1 = f"""create table if not exists `{project_id}`.`nytaxi_stage`.`{table_name}`
    ({col_param})
    """
    q2 = f"""insert into `{project_id}`.`nytaxi_stage`.`{table_name}`
    ({col_names})
    select *, '{root_path}', current_timestamp() from `{project_id}.nytaxi_raw.external_{table_name}`
    """

    print('populating stage table')
    time.sleep(10)
    client.query(q1)
    time.sleep(10)
    client.query(q2)

    print('loading stage table complete')

if __name__ == '__main__':
    print('parsing input arguments')
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--gcp_id')
    parser.add_argument('--gcp_file_cred')
    parser.add_argument('--trip_name')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    
    args = parser.parse_args()

    # resource substring
    trip_substr = args.trip_name #'yellow'
    
    # date ranges
    start_dt = datetime.strptime(args.start_date,'%Y-%m-%d') #datetime.strptime('2019-01-01','%Y-%m-%d')#
    end_dt = datetime.strptime(args.end_date,'%Y-%m-%d') #datetime.strptime('2019-01-01','%Y-%m-%d')#
    delta = relativedelta(months=1)
    
    # var that stays the same through out run
    table_name = f'{trip_substr}_tripdata'
    time_substr = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    q_history_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    bucket_url = f'gs://original-parquet-url'
    bucket_data = f'{trip_substr}-taxi-data-extract-load'
    project_id = args.gcp_id
    
    print(f"will be fetching parquest for {table_name}, from {start_dt} - {end_dt}")
    
    #########################################local#############################################################################
    # os.system('gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS')
    ###########################################################################################################################
    
    # connecting to bigquery 
    # credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_APPLICATION_CREDENTIALS')) #local
    credentials = service_account.Credentials.from_service_account_file(args.gcp_file_cred) #dataproc
    client = bigquery.Client(credentials = credentials, project = project_id)
    
    # start spark session 
    print('starting spark session')
    
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName('extract-load-spark') \
        .getOrCreate()
    
    while start_dt <= end_dt:
    
        # vars for fetching parquet
        year_month = start_dt.strftime("%Y-%m")
        filename = f"{table_name}_{year_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        
        print(f'starting iteration for {table_name}, {year_month}')
        
        # vars for gcp/bucket
        root_path = f"gs://{bucket_data}/{time_substr}_{table_name}_{year_month}"
        
        ###############################################local run###################################################################
        # df = load_trip_data(spark, url, filename)
        ###############################################data proc run###############################################################
        df = load_trip_data(spark, url, filename, bucket_url)
        ###########################################################################################################################
        if parquet_not_downloaded(df):
            abort_pipeline
            pass
        elif parquet_downloaded(df):
            # fix parquets a bit before the push 
            df = dimension_name_cleanup(df)

            # push parquets to bucket
            #################################################local run#################################################################
            # data_2_gcp_cloud_storage(df, table_name, year_month, root_path, filename)
            ############################################dataproc run###################################################################
            data_2_gcp_cloud_storage(df, root_path)
            ###########################################################################################################################

            # customize variables a bit 
            if 'yellow_tripdata_2009' in root_path:
                table_name_q1 = f'external_{table_name}' #external tbl
                table_name_q2 = f'{table_name}_2009' #stage tbl
                data_schema_dict = schema_yellow_dict2009
            elif 'yellow_tripdata_2010' in root_path:
                table_name_q1 = f'external_{table_name}'
                table_name_q2 = f'{table_name}_2010'
                data_schema_dict = schema_yellow_dict2010
            elif 'yellow_tripdata' in root_path:
                table_name_q1 = f'external_{table_name}'
                table_name_q2 = f'{table_name}'
                data_schema_dict = schema_yellow_dict
            elif 'green' in root_path:
                table_name_q1 = f'external_{table_name}'
                table_name_q2 = f'{table_name}'
                data_schema_dict = schema_green_dict
            elif 'fhv' in root_path:
                table_name_q1 = f'external_{table_name}'
                table_name_q2 = f'{table_name}'
                data_schema_dict = schema_fhv_dict
            elif 'fhvhv' in root_path:
                table_name_q1 = f'external_{table_name}'
                table_name_q2 = f'{table_name}'
                data_schema_dict = schema_fhvhv_dict 

            # stageing data load
            bucket_2_ext_tbl(project_id, table_name, root_path, client)
            # logging stats
            update_log_tbl(schema_log_dict, data_schema_dict, 'nytaxi_raw', table_name_q1, table_name_q1, client, q_log_skeleton, project_id, '', '')   
            # copying data to a stage schema 
            bucket_2_bigquery(data_schema_dict, project_id, table_name, root_path, client)
            # logging stats
            update_log_tbl(schema_log_dict, data_schema_dict, 'nytaxi_stage', table_name_q2, table_name_q2, client, q_log_skeleton, project_id, f"where data_source = '{root_path}'", f"where tbl.data_source = '{root_path}'") 
            #save info in qhistory table 
            client.query(q_history.format(project_id, root_path, q_history_time))
        else:
            print('another issue encountered not yet considered, halting iterations')
            break
        
        start_dt += delta
    
    # close spark session
    spark.stop()