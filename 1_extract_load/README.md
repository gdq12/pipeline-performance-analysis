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

    + `ADD KEY` from service account --> dowload

    + copy json key to `~/git_repoos/pipeline-performance-analysis/1_extract_load`

3. create storage bucket 

    + in the dashboard go to Cloud Storage --> Buckets 

    + `+ CREATE` as name `yellow-taxi-data-extract`

    + for multi-region choose EU based

    + everything else leave as default 

### Repo configurations 

* update variable `GOOGLE_SERVICE_ACC_KEY_FILEPATH` in `PROJECT_NAME/io_config.yml` with docker path of the json file 

### Helpful links

* repo setup [guide](https://docs.mage.ai/production/ci-cd/local-cloud/repository-setup)

* quickstart [guide](https://docs.mage.ai/getting-started/setup) for project setup 