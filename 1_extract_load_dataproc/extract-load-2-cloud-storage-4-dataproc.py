# needed libararies 
import os
import re
import argparse
import logging 
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions
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
    logging.info("faulty parquet file downloaded, iteration run aborted")

def load_trip_data(spark, url, filename, bucket_name):
    
    # down load parquet
    logging.info(f'fetching {filename} from {url}')
    os.system(f'curl -O {url}') 
    os.system(f'gsutil -m cp {filename} gs://{bucket_name}/original_parquets_url/')
    
    try:
        # read parquet into environment 
        df = spark.read \
            .option("header", "true") \
            .parquet(f"gs://{bucket_name}/original_parquets_url/{filename}")
        logging.info(f'loaded gs://{bucket_name}/original_parquets_url/{filename} in spark session with dimensions ({df.count()}, {len(df.columns)})')
        return df
    except Py4JJavaError as e:
        # os.system(f"rm -r {filename}")
        logging.info(f"aborted pipeline for gs://{bucket_name}/original_parquets_url/{filename} due to error")
        return None

def dimension_name_cleanup(df):
    logging.info(f"Spark DF currently has the following columns: {', '.join(df.columns)}")
    # cleanup column names 
    for i in range(len(df.columns)):
        col_name = df.columns[i]
        col_name_new = re.sub('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', '_', col_name).lower()
        df = df.withColumnRenamed(col_name, col_name_new) 
    logging.info(f"Spark DF has been updated with the following columns: {', '.join(df.columns)}")
    return df

def data_2_gcp_cloud_storage(df, root_path):
    # repartition df for export 
    pickup_col = [col for col in df.columns if 'pickup' in col][0]
    col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
    df = df.alias('a') \
            .select('a.*', \
                    functions.date_trunc('day', f'a.{pickup_col}') \
                    .alias(col_name))
    
    logging.info(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

    # repartition and save locally 
    os.system(f'gcloud storage folders create {root_path}')

    logging.info(f"loading parquets to {root_path}")
    
    df.repartition(col_name) \
        .write.parquet(root_path, mode = 'overwrite') 
    
    logging.info(f"loading partitions to {root_path} complete")


if __name__ == '__main__':
    # print(os.system('pwd'))
    logging.info('parsing input arguments')
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--table_name')
    parser.add_argument('--start_date')
    parser.add_argument('--end_date')
    
    args = parser.parse_args()
    
    # date ranges
    start_dt = datetime.strptime(args.start_date,'%Y-%m-%d')
    end_dt = datetime.strptime(args.end_date,'%Y-%m-%d')
    delta = relativedelta(months=1)
    
    # var that stays the same through out run 
    table_name = args.table_name
    bucket_name = 'taxi-data-extract'
    
    logging.info(f"wil be fetching parquest for {table_name}, from {start_dt} - {end_dt}")
    
    # start spark session 
    logging.info('starting spark session')
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName('extract-load-spark') \
        .getOrCreate()
        
    while start_dt <= end_dt:
    
        # vars for fetching parquet
        year_month = start_dt.strftime("%Y-%m")
        filename = f"{table_name}_{year_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        
        logging.info(f'starting iteration for {table_name}, {year_month}')
        
        # vars for gcp 
        root_path = f"gs://{bucket_name}/{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}_{table_name}_{year_month}"
        
        # running throught the ETL pipeline
        df = load_trip_data(spark, url, filename, bucket_name)
        if parquet_not_downloaded(df):
            abort_pipeline
            pass
        elif parquet_downloaded(df):
            df = dimension_name_cleanup(df)
            data_2_gcp_cloud_storage(df, root_path)
        else:
            logging.info('another issue encountered not yet considered, halting iterations')
            break
    
        start_dt += delta
    
    # close spark session
    spark.stop()