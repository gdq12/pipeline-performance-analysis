import os
import re
import logging 
import time
import pandas as pd 
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from py4j.protocol import Py4JJavaError

def load_trip_data_local(spark, url, filename):
    
    # down load parquet
    logging.info(f'fetching {filename} from {url}')
    os.system(f'curl -O {url}') 
    
    try:
        # read parquet into environment 
        df = spark.read \
            .option("header", "true") \
            .parquet(filename)
        logging.info(f'loaded {filename} in spark session with dimensions ({df.count()}, {len(df.columns)})')
        return df
    except Py4JJavaError as e:
        os.system(f"rm -r {filename}")
        logging.info(f"{filename} not a valid parquet file, aborted pipeline!!!!") 
        return None

def load_trip_data_dataproc(spark, url, filename, bucket_url):
    
    # down load parquet
    logging.info(f'fetching {filename} from {url}')
    # os.system(f'curl -O {url}') 
    # os.system(f'gsutil -m cp {filename} {bucket_url}')
    
    try:
        # read parquet into environment 
        df = spark.read \
            .option("header", "true") \
            .parquet(f"{bucket_url}/{filename}")
        logging.info(f'loaded {bucket_url}/{filename} in spark session ')
        return df
    except Py4JJavaError as e:
        logging.info(f"aborted pipeline for {bucket_url}/{filename} due to error")
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
    
def data_2_gcp_cloud_storage_local(df, parq_sybstr, year_month, root_path, filename):
    
    # repartition df for export and add column
    pickup_col = [col for col in df.columns if 'pickup' in col][0]
    col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
    df = df.alias('a') \
            .select('a.*', \
                    F.date_trunc('day', f'a.{pickup_col}') \
                    .alias(col_name)) \
            .withColumn("data_source", lit(root_path))
    
    logging.info(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

    # repartition and save locally 
    os.system(f'mkdir {parq_sybstr}_{year_month}')

    logging.info(f"saving partitions in {parq_sybstr}_{year_month}")
    
    df.repartition(col_name) \
        .write.parquet(f'{parq_sybstr}_{year_month}', mode = 'overwrite') 

    # authenticate connex to cloud storage
    os.system('gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS')

    logging.info(f"loading parquets to {root_path}")
    
    # copy parquets to cloud storage
    os.system(f'gsutil -m cp -r {parq_sybstr}_{year_month}/*.parquet {root_path}')
    
    logging.info('cleaning up environment for next run')
    # remove parquets locally to make sure pull from gcp worked
    os.system(f'rm -r {parq_sybstr}_{year_month}')
    os.system(f'rm {filename}')
    
def data_2_gcp_cloud_storage_dataproc(df, root_path):
    
    # repartition df for export and add columns 
    pickup_col = [col for col in df.columns if 'pickup' in col][0]
    col_name = [re.sub('datetime|date_time', 'date', col) for col in df.columns if 'pickup' in col][0]
    
    df = df.alias('a') \
            .select('a.*', \
                    F.date_trunc('day', f'a.{pickup_col}') \
                    .alias(col_name)) \
            .withColumn("data_source", lit(root_path))
    
    logging.info(f"number of unique partitions for DF: {df.select(col_name).distinct().count()}")

    # repartition and save locally 
    os.system(f'gcloud storage folders create {root_path}')

    logging.info(f"loading parquets to {root_path}")
    
    df.repartition(col_name) \
        .write.parquet(root_path, mode = 'overwrite')

    logging.info(f'removing un-needed _SUCCESS file from folder')
    os.system(f'gsutil rm -r {root_path}/_SUCCESS')
    
    logging.info(f"loading partitions to {root_path} complete")

    logging.info('cleaning up environment for next run')
    
def bucket_2_ext_tbl(project_id, ext_table_name, root_path, client):
    
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

    q1e = f"""create schema if not exists `{project_id}`.`nytaxi_monitoring`
    options (location = 'EU')
    """

    q2 = f"""create or replace external table `{project_id}`.`nytaxi_raw.{ext_table_name}`
    options (
    format = 'PARQUET',
    uris = ['{root_path}/*']
    )
    """
    
    # execute queries 
    logging.info('creating if not already present schemas')
    time.sleep(3)
    client.query(q1a)
    time.sleep(3)
    client.query(q1b)
    time.sleep(3)
    client.query(q1c)
    time.sleep(3)
    client.query(q1d)

    logging.info('creating external table')
    time.sleep(3)
    client.query(q2)

    logging.info('loading external table complete')
    
def bucket_2_bigquery(client, project_id, ext_table_name, schema_raw, schema_stage, parq_subset, filename):

    logging.info(f'determining how {ext_table_name} can be loaded to stage')
          
    # see if the most recent stage table can be used to add the external data 
    q1 = """with ext as 
    (SELECT column_name, data_type, ordinal_position
    FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS
    where table_name = '{}'
    ),
    stg as 
    (SELECT column_name, data_type, ordinal_position
    FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS
    where table_name in (select table_name 
                        from `{}`.`{}`.INFORMATION_SCHEMA.TABLES
                        where regexp_substr(table_name, '{}') is not null
                        order by creation_time desc
                        limit 1)
    and column_name != 'creation_dt'
    )
    select ext.column_name, ext.data_type, ext.ordinal_position
    from ext ext 
    left join stg stg on ext.column_name = stg.column_name 
                      and ext.data_type = stg.data_type 
                      and ext.ordinal_position = stg.ordinal_position
    where stg.column_name is null
    """
    
    time.sleep(3)
    df_compare = client.query(q1.format(project_id, schema_raw, ext_table_name,
                                        project_id, schema_stage,
                                        project_id, schema_stage, parq_subset)).to_dataframe()
    
    
    logging.info('compiling the column/datatype specs to push data to stage')
    # get colname and datatype combos from external tale to create the schema dictionary
    q2 = """SELECT column_name, data_type
    FROM `{}`.`{}`.INFORMATION_SCHEMA.COLUMNS
    where table_name = '{}'
    """
    
    time.sleep(3)
    df_tbl = client.query(q2.format(project_id, schema_raw, ext_table_name)).to_dataframe()
    # add column that will be present in the stage table 
    df_tbl = pd.concat([df_tbl, 
                        pd.DataFrame({'column_name': ['creation_dt'], 
                                      'data_type': ['TIMESTAMP']})], 
                       ignore_index = True) 
    df_dict = {key: value for key, value in zip(df_tbl.to_dict('series')['column_name'],df_tbl.to_dict('series')['data_type'])}
    
    col_params = ' '.join([key + ' ' + item + ',' for key, item in df_dict.items()])[:-1]
    col_names = ' '.join([key + ',' for key in df_dict.keys()])[:-1]
    
    if df_compare.shape[0] == 0:
        # get the table name already in stage 
        logging.info(f'there is no change in col_name/data_type/oridinal_position, data will be pushed to latest {parq_subset} table in stage')
        q3a = """select table_name 
                from `{}`.`{}`.INFORMATION_SCHEMA.TABLES
                where regexp_substr(table_name, '{}') is not null
                order by creation_time desc
                limit 1
                """
        
        time.sleep(3)
        table_name = client.query(q3a.format(project_id, schema_stage, parq_subset)).to_dataframe()['table_name'][0]
        # insert records into table                        
        q3b = """insert into `{}`.`nytaxi_stage`.`{}`
        ({})
        select *, current_timestamp() from `{}.nytaxi_raw.{}`
        """
        
        time.sleep(3)
        client.query(q3b.format(project_id, table_name, col_names, project_id, ext_table_name))
    else:
        table_name = filename.replace('.parquet', '')
        cluster_col = 'data_source'
        logging.info(f'there is a change in col_name/data_type/oridinal_position, {table_name} in stage will be created')
        # create new table in stage 
        q3a = """create table if not exists `{}`.`nytaxi_stage`.`{}`
        ({})
        cluster by {} as 
        select *, current_timestamp() from `{}.nytaxi_raw.{}`
        """
        time.sleep(3)
        client.query(q3a.format(project_id, table_name, col_params, cluster_col, project_id, ext_table_name))

    logging.info(f'data load to stage complete')