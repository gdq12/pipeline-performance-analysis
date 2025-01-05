## Method1: Using MAGE.AI

* signitificant dev time went into this one. Encountered issues with data volume with pyarrow and parquets with ~2 million rows or more, require to integrate spark instead of using pandas. This is a to do for the future

### Starting with the project 

1. setup `Dockerfile`, `docker-compose.yml`, `.env` as suggested by the referenced links below 

2. build docker container and spin them up 

    ```
    cd ~/git_repos/pipeline-performance-analysis

    docker compose build

    docker compose up
    ```

3. go to `http://localhost:6789/` to start using mage UI

### Setting up GCP configurations 

1. create project `pipeline-analysis`

2. create a service account 

    + go to IAM & Admin page --> service accounts 

    + `+ CREATE SERVICE ACCOUNT`

    + assign name `mage-extract-load` and add optional description 

    + grant permissions: Artifact Registry Read, Artifact Registry Writer, Cloud Run Developer, Cloud SQL, Service Account Token Creator

        - gcloud syntax: roles/artifactregistry.reader, roles/artifactregistry.writer, roles/cloudsql.admin, roles/iam.serviceAccountTokenCreator, roles/run.developer, roles/secretmanager.secretAccessor, roles/storage.admin, roles/bigquery.dataOwner, roles/iam.serviceAccountAdmin

    + `ADD KEY` from service account --> dowload

    + copy json key to `~/git_repoos/pipeline-performance-analysis/1_extract_load`

3. create storage bucket 

    + in the dashboard go to Cloud Storage --> Buckets 

    + `+ CREATE` as name `taxi-data-extract`

    + for multi-region choose EU based

    + everything else leave as default 

4. same instructions as step 3 for creating `mage-run-logs` bucket

### Repo configurations 

* update variable `GOOGLE_SERVICE_ACC_KEY_FILEPATH` in `PROJECT_NAME/io_config.yml` with docker path of the json file 

### Terraform imlementation with Mage

* terraform files reside in [gcp/](gcp/)

* **very** loosly based on `*.tf` files provided by [Mage template repo](https://github.com/mage-ai/mage-ai-terraform-templates), have been slowly building the files from scratch to see what is and is not necessary for the configurations 

* have not been successfully able to create resources yet since there seems to be a permission error, this will be revisited in the future

* commands for terraform to create resources in GCP:

    ```
    # login and authenticate gcp creds (follow links and provide authentication codes when prompted)
    gcloud auth login
    gcloud auth application-default login

    # to initiate terraform 
    terraform init

    # to see what new resources will be buit
    terraform plan

    # to deploy resource creation
    terraform apply

    # to tear down resources when no longer needed 
    terraform destroy
    ```

## Method2: Using Spark Cluster from GCP DataProc

* this is an intermediate solution to dev and test out spark syntax for loading parquets

* the syntax here will be integrated in Mage blocks in the future 

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

* all dev work can be found in [spark_pipeline/](spark_pipeline/)

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
* steps to execute `extract-load-2-cloud-storage-4-local.py` in docker: 

    ```
    # docker build command 
    docker build -t extract-load-spark:latest .
    # docker run command 
    docker run --rm -it -p 8888:8888 -e ROOT=TRUE extract-load-spark

    # go to jupyter notebook UI
    localhost:8888

    # update the input parameters accordingly
    python3 extract-load-2-cloud-storage-4-local.py \
        --table_name yellow_tripdata \
        --start_date 2008-12-01 \
        --end_date 2009-01-01

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
    sudo gcloud services list --enabled --project pipeline-analysis-446021
    ```

* current IAM policies for all accounts in project 

    ```
    sudo gcloud projects get-iam-policy pipeline-analysis-446021
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