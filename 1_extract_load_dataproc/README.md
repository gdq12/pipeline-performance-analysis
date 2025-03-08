## Background

This part of the project is to fullfill the E (extract) and L (load) of ELT of the NYC taxi data into Bigquery. Apache Spark clusters via GCP Dataproc are used for the efficient large data transfers. The DRY (dont repeat yourself) method is carried out here and is the foundational code to be used for spark implementation in Mage in the future. 

### Google Cloud Platform Setup

* GCP centric identifiers have been save in `.envrc` file:

    ```{bash}
    export PROJECT_NAME=""
    export PROJECT_NUMBER=""
    export PROJECT_ID=""
    export PROJECT_KEY_PATH=""
    export CLUSTER_REGION=""
    export PROJECT_EMAIL=""
    export SPARK_SERVICE_ACCOUNT=""
    ```

1. create project: `PROJECT_NAME`

2. create a service account 

    + go to IAM & Admin page --> service accounts 

    + `+ CREATE SERVICE ACCOUNT`

    + assign name `mage-extract-load` and add optional description 

    + grant permissions: owner

    + `ADD KEY` from service account --> dowload

    + save json key to `~/Documents/` and `~/git_repos/pipeline-performance-analysis/1_extract_load_dataproc`

3. create storage bucket 

    + in the dashboard go to Cloud Storage --> Buckets 

    + `+ CREATE` as name: 
        
        - `spark-scripts-extract-load`

        - `original-parquet-url`

        - `yellow-taxi-data-extract-load`

        - `green-taxi-data-extract-load`

        - `fhv-taxi-data-extract-load`

        - `fhvhv-taxi-data-extract-load`

    + for multi-region choose EU based

    + everything else leave as default 

### Docker for local development 

create local docker image from docker hub `spark:3.5.1-scala2.12-java11-python3-ubuntu` for local testing prior to pushing to dataproc. Commands can be found below 

```
# docker build command 
docker build -t extract-load-spark:latest .

# docker run command 
docker run --rm -it \
    -p 8888:8888 \
    -e ROOT=TRUE \
    -e GOOGLE_APPLICATION_CREDENTIALS="/home/ubuntu/${PROJECT_KEY_PATH}" \
    -v ~/git_repos/pipeline-performance-analysis/1_extract_load_dataproc/${PROJECT_KEY_PATH}:/home/ubuntu/${PROJECT_KEY_PATH} \
    -v ~/git_repos/pipeline-performance-analysis/1_extract_load_dataproc/extract-load-2-cloud-storage.py:/home/ubuntu/extract-load-2-cloud-storage.py \
    -v ~/git_repos/pipeline-performance-analysis/1_extract_load_dataproc/dict_query_helpers.py:/home/ubuntu/dict_query_helpers.py \
    extract-load-spark:latest

# go to jupyter notebook UI
localhost:8888

# command for local run
python3 extract-load-2-cloud-storage.py \
    --table_name yellow_tripdata \
    --start_date 2019-01-01 \
    --end_date 2019-07-01 \
    --project_id ${PROJECT_ID }\
    --trip_name yellow

```

-- to be continued
### Google Cloud Platforms Dataproc w/Spark

1. in GCP dashboard search for dataproc API --> enable it

2. go to Dataproc page and create a cluster:

  + `+ Create Cluster`

  + `Cluster on Computer Engine`

  + fill out fields:

    * set up cluster tab:

      - Name: `extract-load-spark`

      - Region: `europe-west10` (same region as bucket)

      - Cluster type: Single Node (use this when working with a small amount of data)

      - optional components: jupyter notebook, docker

    * customize cluster (optional) tab:

      - uncheck "Configure all instances to have only internal IP address"

  + good to know: can also see the cluster instance in `VM Instances` page of the dashboard

  + from that dashboard can also see where all log info is stored within cloud storage

### Steps to execute script in DataProd cluster

* the scripts `extract-load-2-cloud-storage*.py` mimic the same concepts as the Mage pipelines but with spark syntax. There is also a while loop to mimic the backfill mechanism that Mage performs under the hood 

* steps to execute `extract-load-2-cloud-storage-4-dataproc.py` in GCP:

    ```
    # login and authenticate gcp creds (follow links and provide authentication codes when prompted)
    gcloud auth login
    gcloud auth application-default login

    # cp command pythong script to cloud storage
    gsutil cp extract-load-2-cloud-storage-4-dataproc.py gs://taxi-data-extract/spark-scripts/extract-load-2-cloud-storage-4-dataproc.py

    # grant permissions for cmd line execution
    gcloud projects add-iam-policy-binding pipeline-analysis-446021 \
        --member=serviceAccount:1023261528910-compute@developer.gserviceaccount.com \
        --role="roles/bigquery.jobUser"

    # update last 3 lines accordingly
    gcloud dataproc jobs submit pyspark \
        --cluster=extract-load-spark \
        --region=europe-west10 \
        --jars gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar  \
        gs://taxi-data-extract/spark-scripts/extract-load-2-cloud-storage-4-dataproc.py \
        -- \
        --table_name=fhvhv_tripdata \
        --start_date=2019-02-01 \
        --end_date=2024-10-01
    ```

### Copying the data over to stage


1. remove `_SUCCESS` files from each of subfolders so there are only `.parquet` files 

    ```
    sudo gsutil rm -r gs://taxi-data-extract/*/_SUCCESS
    ```

## GCLOUD commands good to knows 

* getting the roles current assigned to the service account

    ```
    sudo gcloud projects get-iam-policy pipeline-analysis-446021 \
    --flatten="bindings[].members" \
    --format='table(bindings.role)' \
    --filter="bindings.members:mage-extract-load@pipeline-analysis-446021.iam.gserviceaccount.com"
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

--properties dataproc:dataproc.monitoring.stackdriver.enable=true

### Helpful links

* repo setup [guide](https://docs.mage.ai/production/ci-cd/local-cloud/repository-setup)

* quickstart [guide](https://docs.mage.ai/getting-started/setup) for project setup 

* gcp + terraform [guid](https://docs.mage.ai/production/deploying-to-cloud/gcp/setup)

* repo notes that helped a bit with terraform basics: [terraform basics](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week1/1_2_gcp_terraform/3_terraform_basics), [terraform variables](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week1/1_2_gcp_terraform/4_terraform_variables)

* while loop concept for getting parquet to cloud storage: [parquets to gcp](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/url_2_gcs.py)

* helpful queries that helped with loading data to Bigquery: [sql script](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/gcs_2_bigquery.sql)

* notes that helped with local and dataproc spark dev: [5_11](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_11_create_local_cluster), [5_12](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_12_spark_cluster_gcp) and [5_13](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_13_spark_dataproc_bigquery)