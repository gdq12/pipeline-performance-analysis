### Current Notes

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

### Helpful links

* repo setup [guide](https://docs.mage.ai/production/ci-cd/local-cloud/repository-setup)

* quickstart [guide](https://docs.mage.ai/getting-started/setup) for project setup 

* gcp + terraform [guid](https://docs.mage.ai/production/deploying-to-cloud/gcp/setup)

* repo notes that helped a bit with terraform basics: [terraform basics](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week1/1_2_gcp_terraform/3_terraform_basics), [terraform variables](https://github.com/gdq12/data-engineering-zoomcamp-2024/tree/main/week1/1_2_gcp_terraform/4_terraform_variables)