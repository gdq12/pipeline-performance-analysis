## Step I: create DBT service account in GCP

+ go to IAM & Admin page --> service accounts 

+ `+ CREATE SERVICE ACCOUNT`

+ assign name `transformation-dbt` and add optional description 

+ grant permissions: `BigQuery Admin`

+ `ADD KEY` from service account --> dowload

+ save json key to `~/Documents/` and `${LOCAL_WORKING_DIRECTORY}`

## Step II: Setup devcontainer

* `.envrc`

    ```
    export SCRIPTS_REPO=""
    export LOCAL_WORKING_DIRECTORY=""
    ```

* use either one for limne 5 in [.devcontainer/devcontainer.json](.devcontainer/devcontainer.json)

* `.devcontainer/devcontainer.env`: these are vars to be used for `profile.yml` when using docker container for local dev

    ```
    PROJECT_NAME=
    PROJECT_NUMBER=
    PROJECT_ID=
    PROJECT_KEY=
    BQ_REGION=
    BQ_SCHEMA_PREFIX=
    BQ_RAW_SCHEMA=
    PROJECT_EMAIL=
    ```

## Step III: Build docker image and launch container

```
# build docker image
docker build -t pipeline-analysis-transform-dbt .

# launch with VS code 
source .envrc
code .
```