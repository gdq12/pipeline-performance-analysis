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

### Repo configurations 

* update variable `GOOGLE_SERVICE_ACC_KEY_FILEPATH` in `PROJECT_NAME/io_config.yml` with docker path of the json file 

### GCLOUD commands good to knows 

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


### Helpful links

* repo setup [guide](https://docs.mage.ai/production/ci-cd/local-cloud/repository-setup)

* quickstart [guide](https://docs.mage.ai/getting-started/setup) for project setup 

g://taxi-data-extract/spark-scripts/extract-load-2-cloud-storage.py

```
# login and authenticate gcp creds (follow links and provide authentication codes when prompted)
gcloud auth login
gcloud auth application-default login

# cp command
gsutil cp extract-load-2-cloud-storage.py g://taxi-data-extract/spark-scripts/extract-load-2-cloud-storage.py
```

```{bash}
gcloud projects add-iam-policy-binding pipeline-analysis-446021 \
    --member=serviceAccount:1023261528910-compute@developer.gserviceaccount.com \
    --role="roles/bigquery.jobUser"

```

``` {bash}
gcloud dataproc jobs submit pyspark \
    --cluster=extract-load-spark \
    --region=europe-west10 \
    --jars gs://spark-lib/bigquery/spark-3.4-bigquery-0.37.0.jar  \
    g://taxi-data-extract/spark-scripts/extract-load-2-cloud-storage.py \
    -- \
    --table_name=yellow_tripdata \
    --start_date=2009-01-01 \
    --end_date=2009-01-01
```

python3 extract-load-2-cloud-storage.py \
    --table_name yellow_tripdata \
    --start_date 2009-01-01 \
    --end_date 2009-01-01