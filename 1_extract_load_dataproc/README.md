## Background

This part of the project is to fullfill the E (extract) and L (load) of ELT of the NYC taxi data into Bigquery. Apache Spark clusters via GCP Dataproc are used for the efficient large data transfers. The DRY (dont repeat yourself) method is carried out here and is the foundational code to be used for spark implementation in Mage in the future. 

## Step I: Google Cloud Platform Setup

* source GCP centric identifiers have been save in `.envrc` file (via command `source .envrc`):

    ```{bash}
    export LOCAL_WORKING_DIRECTORY=""
    export PROJECT_NAME=""
    export PROJECT_NUMBER=""
    export PROJECT_ID=""
    export PROJECT_KEY_PATH=""
    export CLUSTER_REGION=""
    export PROJECT_EMAIL=""
    export SPARK_SERVICE_ACCOUNT=""
    export COMPUTE_ENGINE_ACCOUNT=""
    ```

1. create project: `PROJECT_NAME`

2. create a service account 

    + go to IAM & Admin page --> service accounts 

    + `+ CREATE SERVICE ACCOUNT`

    + assign name `extract-load-spark` and add optional description 

    + grant permissions: owner

    + `ADD KEY` from service account --> dowload

    + save json key to `~/Documents/` and `${LOCAL_WORKING_DIRECTORY}/`

3. create storage bucket 

    + in the dashboard go to Cloud Storage --> Buckets 

    + `+ CREATE` as name: 
        
        - `spark-scripts-extract-load`

        - `original-parquet-url`

        - `helper-data`

        - `yellow-taxi-data-extract-load`

        - `green-taxi-data-extract-load`

        - `fhv-taxi-data-extract-load`

        - `fhvhv-taxi-data-extract-load`

    + for multi-region choose EU based

    + everything else leave as default 

## Step 2a: Docker for local development 

Created local docker image from docker hub `spark:3.5.1-scala2.12-java11-python3-ubuntu` for local testing prior to pushing to dataproc. Commands can be found below 

```
# docker build command 
docker build -t extract-load-spark:latest .

# docker run command 
docker run --rm -it \
    -p 8888:8888 \
    -e ROOT=TRUE \
    -e GOOGLE_APPLICATION_CREDENTIALS="/home/ubuntu/${PROJECT_KEY_PATH}" \
    -e PROJECT_ID=${PROJECT_ID} \
    -v ${LOCAL_WORKING_DIRECTORY}/${PROJECT_KEY_PATH}:/home/ubuntu/${PROJECT_KEY_PATH} \
    -v ${LOCAL_WORKING_DIRECTORY}/extract-load-2-cloud-storage.py:/home/ubuntu/extract-load-2-cloud-storage.py \
    -v ${LOCAL_WORKING_DIRECTORY}/helper_funcs.py:/home/ubuntu/helper_funcs.py \
    -v ${LOCAL_WORKING_DIRECTORY}/dict_query_helpers.py:/home/ubuntu/dict_query_helpers.py \
    extract-load-spark:latest

# go to jupyter notebook UI
localhost:8888

# command for local run
python3 extract-load-2-cloud-storage.py \
    --trip_name yellow_tripdata \
    --start_date 2010-12-01 \
    --end_date 2011-02-01 \
    --gcp_id ${PROJECT_ID}\
    --trip_name yellow 
```

## Step 2b: Google Cloud Platforms Dataproc w/Spark

### Method I: Via GCP concole UI

**Method a bit easier but didnt seem to offer the option to enable job logging**

1. in GCP dashboard search for dataproc API --> enable it

2. go to Dataproc page and create a cluster:

  + `+ Create Cluster`

  + `Cluster on Computer Engine`

  + fill out fields:

    * set up cluster tab:

      - Name: `extract-load-spark`

      - Region: `${CLUSTER_REGION}` (same region as bucket)

      - Cluster type: Single Node (use this when working with a small amount of data)

      - optional components: jupyter notebook, docker

    * customize cluster (optional) tab:

      - uncheck "Configure all instances to have only internal IP address"

  + good to know: can also see the cluster instance in `VM Instances` page of the dashboard

  + from that dashboard can also see where all log info is stored within cloud storage

### Method II: Via command line

**Command below was compiled from Method I, where it provides the "Equivalent in command line" option prior to execution**

1. enable `Cloud Resource Manager API` in [api library page](https://console.cloud.google.com/apis/library)

2. apply permissions needed for the cloud cluster service account 

    ```
    sudo gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member=serviceAccount:${COMPUTE_ENGINE_ACCOUNT} \
        --role="roles/bigquery.jobUser"

    sudo gcloud projects add-iam-policy-binding ${PROJECT_ID} \
        --member=serviceAccount:${COMPUTE_ENGINE_ACCOUNT} \
        --role="roles/roles/storage.admin"
    ```

3. create cluster via GCP UI power shell (there was a zone configuartion issue when attempted to sudo execute the command locally)

    ```
    gcloud dataproc clusters create extract-load-spark \
        --properties dataproc:dataproc.monitoring.stackdriver.enable=true \
        --enable-component-gateway \
        --region=${CLUSTER_REGION} \
        --single-node \
        --master-machine-type n2-standard-4 \
        --master-boot-disk-type pd-balanced \
        --master-boot-disk-size 100 \
        --image-version 2.2-debian12 \
        --optional-components JUPYTER,DOCKER \
        --project ${PROJECT_ID}
    ```

4. Work arounds

* had issues with downloading parquets to VM in dataproc: `curl: (28) Failed to connect to d37ci6vzurychx.cloudfront.net port 443 after 300714 ms: Timeout was reached`

* bit of googling arund led to believe that it was a firewall issue [stackoverflow post](https://stackoverflow.com/questions/44876177/port-443-is-close-in-google-cloud-instance), [another post](https://scrapfly.io/blog/what-is-the-curl-28-error/)

* in the end this could not easily be resolved and chose not to invest more time into ammending this issue, therefore just added the parquets to the bucket locally via the following python script:

    ```{python}
    import os
    import argparse
    import time
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    bucket_url = f'gs://original-parquet-url'
    table_name = f'{green}_tripdata'
    start_dt = datetime.strptime('2009-01-01','%Y-%m-%d')
    end_dt = datetime.strptime('2024-12-01','%Y-%m-%d')
    delta = relativedelta(months=1)

    while start_dt <= end_dt:
        year_month = start_dt.strftime("%Y-%m")
        filename = f"{table_name}_{year_month}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"

        print(f'fetching {filename} from {url}')
        os.system(f'curl -O {url}') 

        print(f'pushing {filename} to {bucket_url}')
        os.system(f'gsutil -m cp {filename} {bucket_url}')

        print(f'cleaning up env, removing {filename} locally')
        os.system(f'rm {filename}')

        start_dt += delta
    ```

* notes for availble data by start date:

    - yellow: 2009-01-01

    - green: 2013-08-01 (but seems like parquet file isnt an actual size till 2014-01-01)
     
    - fhv: 2015-01-01

    - fhvhv: 2019-02-01

5. Execute scripts in Dataproc

* copy python scripts to script bucket 

    ```
    sudo gsutil cp extract-load-2-cloud-storage.py gs://spark-scripts-extract-load/extract-load-2-cloud-storage.py

    sudo gsutil cp helper_funcs.py gs://spark-scripts-extract-load/helper_funcs.py

    sudo gsutil cp dict_query_helpers.py gs://spark-scripts-extract-load/dict_query_helpers.py

    sudo gsutil cp ${PROJECT_KEY_PATH} gs://spark-scripts-extract-load/${PROJECT_KEY_PATH}
    ```

* trigger job per needed paramters

    ```
    sudo gcloud dataproc jobs submit pyspark \
        --cluster=extract-load-spark \
        --region=${CLUSTER_REGION} \
        --jars gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar  \
        --py-files gs://spark-scripts-extract-load/dict_query_helpers.py,gs://spark-scripts-extract-load/helper_funcs.py \
        --files ${PROJECT_KEY_PATH} \
        gs://spark-scripts-extract-load/extract-load-2-cloud-storage.py \
        -- --gcp_id=${PROJECT_ID} \
        --trip_name=yellow \
        --start_date=2009-01-01 \
        --end_date=2024-12-01 \
        --gcp_file_cred=${PROJECT_KEY_PATH}
    ```

## Step 3: Adding supplimentary tables to Bigquery 

1. load csv files in [mapping_tbl_csv](mapping_tbl_csv) to the `helper-data` bucket 

    *can be done either by gutil are drag and drop in UI*

2. execute queries in [extract-load-mapping-tables.sql](extract-load-mapping-tables.sql) to load them in the desrired schema

## GCLOUD commands good to knows 

* getting the roles current assigned to the service account

    ```
    sudo gcloud projects get-iam-policy ${PROJECT_ID} \
    --flatten="bindings[].members" \
    --format='table(bindings.role)' \
    --filter="bindings.members:${COMPUTE_ENGINE_ACCOUNT}"
    ```

* current enabled API for project 

    ```
    sudo gcloud services list --enabled --project ${PROJECT_ID}
    ```

* current IAM policies for all accounts in project 

    ```
    sudo gcloud projects get-iam-policy ${PROJECT_ID}
    ```

* get list of service accounts 

    ```
    sudo gcloud iam service-accounts list
    ```

* list files in cloud storage with specific syntax

    ```
    sudo gcloud storage ls --recursive gs://taxi-data-extract/*/_SUCCESS
    ```

* to remove files with specific pattern

    ```
    sudo gsutil rm -r gs://taxi-data-extract/*/_SUCCESS
    ```

* to remove old account from gcloud config

    ```
    gcloud auth revoke old.main.account@gmail.com
    ```

* list projects in account

    ```
    gcloud projects list
    ```

* update project setting for gcloud

    ```
    sudo gcloud config set project ${PROJECT_ID}
    ```

* login to use gcloud command line

    ```
    gcloud auth login --no-launch-browser
    gcloud auth login
    gcloud auth application-default login

    ```

* to make sure that working with right user in command line 

    ```
    sudo gcloud config set account ${PROJECT_EMAIL}
    ```

### Helpful links

* while loop concept for getting parquet to cloud storage: [parquets to gcp](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/url_2_gcs.py)

* helpful queries that helped with loading data to Bigquery: [sql script](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/gcs_2_bigquery.sql)

* notes that helped with local and dataproc spark dev: [5_11](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_11_create_local_cluster), [5_12](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_12_spark_cluster_gcp) and [5_13](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_13_spark_dataproc_bigquery)

* [quick start guide](https://cloud.google.com/dataproc/docs/quickstarts/create-cluster-gcloud) to dataproc

* documentation on dataproc [job output logs](https://cloud.google.com/dataproc/docs/guides/dataproc-job-output#console)

* spark background info on [metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics)

* documentation background on dataproc [metric](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#enable_custom_metric_collection) customization