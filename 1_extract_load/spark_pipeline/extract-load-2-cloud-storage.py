# needed libararies 
import os
import re
import argparse
# import logging 
# import pyspark
from datetime import date
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions
from py4j.protocol import Py4JJavaError

def load_trip_data(spark, url, filename):
    
    # down load parquet
    print(f'fetching {filename} from {url}')
    os.system(f'curl -O {url}') 
    
    try:
        # read parquet into environment 
        df = spark.read \
            .option("header", "true") \
            .parquet(filename)
        print(f'loaded {filename} in spark session with dimensions ({df.count()}, {len(df.columns)})')
        return df
    except Py4JJavaError as e:
        os.system(f"rm -r {filename}")
        raise Exception(f"{filename} not a valid parquet file, aborted pipeline!!!!") from None

def dimension_name_cleanup(df):
    print(f"Spark DF currently has the following columns: {', '.join(df.columns)}")
    # cleanup column names 
    for i in range(len(df.columns)):
        col_name = df.columns[i]
        col_name_new = re.sub('(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z]{1}[a-z])', '_', col_name).lower()
        df = df.withColumnRenamed(col_name, col_name_new) 
    print(f"Spark DF has been updated with the following columns: {', '.join(df.columns)}")
    return df

def data_2_gcp_cloud_storage(df, table_name, year_month, root_path, filename):
    # repartition df for export 
    pickup_col = [col for col in df.columns if 'pickup' in col][0]
    col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
    df = df.alias('a') \
            .select('a.*', \
                    functions.date_trunc('day', f'a.{pickup_col}') \
                    .alias(col_name))
    
    print(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

    # repartition and save locally 
    os.system(f'mkdir {table_name}_{year_month}')

    print(f"saving partitions in {table_name}_{year_month}")
    
    df.repartition(col_name) \
        .write.parquet(f'{table_name}_{year_month}', mode = 'overwrite') 

    # authenticate connex to cloud storage
    os.system('gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS')

    print(f"loading parquets to g//:{root_path}")
    
    # copy parquets to cloud storage
    os.system(f'gsutil -m cp -r {table_name}_{year_month}/*.parquet gs://{root_path}')
    
    print('cleaning up environment for next run')
    # remove parquets locally to make sure pull from gcp worked
    os.system(f'rm -r {table_name}_{year_month}')
    os.system(f'rm {filename}')
    
if __name__ == '__main__':
    print('parsing input arguments')
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

    print(f"wil be fetching parquest for {table_name}, from {start_dt} - {end_dt}")
    
    # start spark session 
    print('starting spark session')
    spark = SparkSession.builder \
        .master("local[4]") \
        .appName('extract-load-spark') \
        .getOrCreate()
    
    while args.start_date <= args.end_date:
    
        # vars for fetching parquet
        year_month = start_dt.strftime("%Y-%m")
        filename = f"{table_name}_{year_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        
        print(f'starting iteration for {table_name}, {year_month}')
    
        # vars for gcp 
        root_path = f"{bucket_name}/{datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}_{table_name}_{year_month}"
    
        # running throught the ETL pipeline
        df = load_trip_data(spark, url, filename)
        df = dimension_name_cleanup(df)
        data_2_gcp_cloud_storage(df, table_name, year_month, root_path, filename)

        # close spark session
        spark.stop()