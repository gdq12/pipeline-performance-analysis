### All account permissions to run project

- in UI

    + compute engine service account (for dataproc): `BigQuery Job User`, `Editor`, `Storage Admin`

    + spark cluster service account (for dataproc): `BigQuery Admin`, `Storage Admin`

    + DBT service account (for transformation): `BigQuery Admin`

- gcloud command line `sudo gcloud projects get-iam-policy ${PROJECT_ID}`

    - members:
        - serviceAccount:extract-load-spark@${PROJECT_ID}.iam.gserviceaccount.com
        - serviceAccount:transformation-dbt@${PROJECT_ID}.iam.gserviceaccount.com
        role: roles/bigquery.admin

    - members:
        - serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com
        role: roles/bigquery.jobUser

    - members:
        - serviceAccount:service-${PROJECT_NUMBER}@compute-system.iam.gserviceaccount.com
        role: roles/compute.serviceAgent

     - members:
        - serviceAccount:service-${PROJECT_NUMBER}@dataproc-accounts.iam.gserviceaccount.com
        role: roles/dataproc.serviceAgent

    - members:
        - serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com
        - serviceAccount:${PROJECT_NUMBER}@cloudservices.gserviceaccount.com
        role: roles/editor

    - members:
        - user:${PROJECT_EMAIL}
        role: roles/owner

     - members:
        - serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com
        - serviceAccount:extract-load-spark@${PROJECT_ID}.iam.gserviceaccount.com
        role: roles/storage.admin

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

* get current account set in gcloud

    ```
    gcloud config list account --format "value(core.account)"
    or 
    sudo gcloud auth list
    ```

* to remove old account from gcloud config

    ```
    gcloud auth revoke old.main.account@gmail.com
    ```

* to set gcloud default account 

    ```
    gcloud config set account ${PROJECT_EMAIL}
    gcloud auth login ${PROJECT_EMAIL}
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
    gcloud init
    gcloud auth login --no-launch-browser
    gcloud auth login
    gcloud auth application-default login

    ```

* to make sure that working with right user in command line 

    ```
    sudo gcloud config set account ${PROJECT_EMAIL}
    ```

* to update gcloud CLI

    ```
    gcloud components update
    ```

### Helpful links

* where the parquet file [links](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) came from

* while loop concept for getting parquet to cloud storage: [parquets to gcp](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/url_2_gcs.py)

* helpful queries that helped with loading data to Bigquery: [sql script](https://github.com/gdq12/data-engineering-zoomcamp-2024/blob/main/week4/4_1a_data_2_gcs/gcs_2_bigquery.sql)

* notes that helped with local and dataproc spark dev: [5_11](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_11_create_local_cluster), [5_12](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_12_spark_cluster_gcp) and [5_13](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week5/5_13_spark_dataproc_bigquery)

* [quick start guide](https://cloud.google.com/dataproc/docs/quickstarts/create-cluster-gcloud) to dataproc

* documentation on dataproc [job output logs](https://cloud.google.com/dataproc/docs/guides/dataproc-job-output#console)

* spark background info on [metrics](https://spark.apache.org/docs/latest/monitoring.html#metrics)

* documentation background on dataproc [metric](https://cloud.google.com/dataproc/docs/guides/dataproc-metrics#enable_custom_metric_collection) customization

* [google article](https://cloud.google.com/blog/products/data-analytics/dataproc-job-optimization-how-to-guide) about dataproc optimization 

* [medium article](https://medium.com/@goyalarchana17/part-1-assessing-spark-cluster-utilization-and-capacity-when-and-how-to-scale-effectively-aa0090f0f888) on interpreting spark cluster performance