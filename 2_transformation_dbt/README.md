## Setup steps 

1. Set up local env for dev work 

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

2. Build and run Docker image 

    ```
    # build docker image
    docker build -t pipeline-analysis-transform-dbt .

    # launch with VS code 
    source .envrc
    code .
    ```

3. create DBT service account in GCP 

    + go to IAM & Admin page --> service accounts 

    + `+ CREATE SERVICE ACCOUNT`

    + assign name `transformation-dbt` and add optional description 

    + grant permissions: `ownerBigQuery Admin`

    + `ADD KEY` from service account --> dowload

    + save json key to `~/Documents/` and `${LOCAL_WORKING_DIRECTORY}`
    
## Good to know DBT commands and more

* commands 

    ```
    # testing dbt connection/installation
    dbt debug

    # compile the query before sending it to Bigquery 
    dbt compile --select "modelName"

    # install dbt dependencies in packages.yml file 
    dbt deps 

    # to build a single table/model
    dbt build --select modelName

    # in dbt Cloud only: to auto create the query configuration macro 
    __config

    # to build models in only certain sub folders and thei dependents
    dbt build --select folder1Name.subfolderName+

    # sourcing data in queries when in subfolders
    source('folder1Name', 'subfolderName', 'tblName')

    # codagen to generate needed yml files 
    dbt run-operation generate_model_yaml --args '{"model_names": ["customers"]}'

    # generate documentation 
    dbt docs generate

    # render documentation 
    dbt docs serve

    dbt run 

    dbt test
    ```

* other

    + post `dbt compile --select "modelName"` can see the compiled sql script in folder: `target/projectName/models/schemaName/`

## Helpful Links 

### Installation related

* installing with docker [page](https://docs.getdbt.com/docs/core/docker-install)

* installing dbt with [pip](https://docs.getdbt.com/docs/core/pip-install)

* using DBT maintained docker image [dbt maintained docker image name](https://github.com/dbt-labs/dbt-bigquery/pkgs/container/dbt-bigquery)

* repo on the different dbt [adaptors](https://github.com/dbt-labs/dbt-adapters?tab=readme-ov-file)

### DBT helpful links

* quick-start [dbt core bigquery](https://docs.getdbt.com/guides/manual-install?step=1)

* documentation on [bigquery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) with dbt

* dbt [schema creation](https://docs.getdbt.com/docs/build/custom-schemas) documentation

* documentation on [incremental models](https://docs.getdbt.com/docs/build/incremental-models)

* doc explaining [jinja](https://docs.getdbt.com/docs/build/jinja-macros) and best practices to apply it 

* [dbt-utls repo](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#get_column_values-source), helpful on seeing how to implement macros into project

* [dbt package hub](https://hub.getdbt.com/)

* dbt package [codagen](https://github.com/dbt-labs/dbt-codegen/tree/0.13.1/) which helps compile code/docs/yml files.

* documentation on [codegen](https://github.com/dbt-labs/dbt-codegen)

* [cross database macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros), dbt functions that transform into SQL DB centric syntax. Best when using project files across different DBs. 

* run_query macro [documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/run_query)

* about [dbt compile](https://docs.getdbt.com/reference/commands/compile)

* good [post](https://discourse.getdbt.com/t/can-i-create-an-auto-incrementing-id-in-dbt/579/3) about **auto-incremental** ID col implementation in dbt

* good post on [limitations of incremental models](https://discourse.getdbt.com/t/on-the-limits-of-incrementality/303)

* post that explains on how to [union](https://discourse.getdbt.com/t/unioning-identically-structured-data-sources/921/2) many source tables

* Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)

* Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers

* Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers

* Find [dbt events](https://events.getdbt.com) near you

* Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Good to know 3rd party DBT links

* [medium article](https://blog.det.life/5-useful-loop-patterns-in-dbt-f1d959ab38b9) on implementing for-loops with jinja

* interesting approach to possibly implement RLS in dbt in this [medium article](https://medium.com/@azart0308/dbt-dynamic-column-selection-macros-4df5faaee42d) 

* [stackoverflow post](https://stackoverflow.com/questions/73157834/change-column-name-dynamically-using-mapping-table-dbt) helped develop for-loop column name concept

* [jinja cheat sheet](https://datacoves.com/post/dbt-jinja-cheat-sheet)

* [macro use-cases](https://www.getorchestra.io/guides/best-dbt-core-macros-examples-and-use-cases)

### Helpful links about the NYC taxi dataset

* taxidataset [data cleaning](https://medium.com/@linniartan/nyc-taxi-data-analysis-part-1-clean-and-transform-data-in-bigquery-2cb1142c6b8b), which also includes how to convert geospatial coordinates to zone_id

* where [data documentation](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) background can be found