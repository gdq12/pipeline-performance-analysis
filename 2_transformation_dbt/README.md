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

    + grant permissions: `BigQuery Admin`

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

    # codagen to generate needed yml files 
    dbt run-operation generate_model_yaml --args '{"model_names": ["customers"]}'
    dbt run-operation copy_clone_raw_tables --args '{"tbl_sustr": '2019|2020|2021'}'

    # generate documentation 
    dbt docs generate

    # render documentation 
    dbt docs serve

    # run all models in layer except for 1
    dbt run --select "models/stage/yellow/" --exclude "stg_yellow__from_source_clean"
    ```

* other

    + post `dbt compile --select "modelName"` can see the compiled sql script in folder: `target/projectName/models/schemaName/`

    + example case to debug macro:

        ```
        {# 
            cmd:  dbt run-operation copy_clone_raw_tables --args '{tbl_substr: "2019|2020|2021"}' --debug

        #}

        {% macro copy_clone_raw_tables(tbl_substr) -%}


        {% set tbl_query %} 
        select table_name
        from `{{ env_var('PROJECT_ID') }}`.`{{ env_var('BQ_RAW_SCHEMA') }}_backup`.INFORMATION_SCHEMA.TABLES
        where regexp_substr(table_name, '{{ tbl_substr }}') is not null
        {% endset %}

        {% set table_names = dbt_utils.get_query_results_as_dict(tbl_query)%}

        {% do log(table_names | tojson, info = True)%}

        {%- endmacro %}

        ```

## Helpful Links 

### Installation related

* installing with docker [page](https://docs.getdbt.com/docs/core/docker-install)

* installing dbt with [pip](https://docs.getdbt.com/docs/core/pip-install)

* using DBT maintained docker image [dbt maintained docker image name](https://github.com/dbt-labs/dbt-bigquery/pkgs/container/dbt-bigquery)

* repo on the different dbt [adaptors](https://github.com/dbt-labs/dbt-adapters?tab=readme-ov-file)

### DBT helpful links

#### Packages

* [dbt-utls repo](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#get_column_values-source), helpful on seeing how to implement macros into project

* [dbt package hub](https://hub.getdbt.com/)

* dbt package [codagen](https://github.com/dbt-labs/dbt-codegen/tree/0.13.1/) which helps compile code/docs/yml files.

* documentation on [codegen](https://github.com/dbt-labs/dbt-codegen)

* [cross database macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros), dbt functions that transform into SQL DB centric syntax. Best when using project files across different DBs. 

#### Working with DBT

* possible [commands](https://docs.getdbt.com/reference/dbt-commands) that can use in dbt

* doc on [node selection](https://docs.getdbt.com/reference/node-selection/syntax), syntax to use to run/build only certain models etc.

* documentation on [dbt artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts)



#### DBT community posts 

* good [post](https://discourse.getdbt.com/t/can-i-create-an-auto-incrementing-id-in-dbt/579/3) about **auto-incremental** ID col implementation in dbt

* good post on [limitations of incremental models](https://discourse.getdbt.com/t/on-the-limits-of-incrementality/303)

* post that explains on how to [union](https://discourse.getdbt.com/t/unioning-identically-structured-data-sources/921/2) many source tables

* [post](https://discourse.getdbt.com/t/faq-i-got-an-unused-model-configurations-error-message-what-does-this-mean/112) about correctly configuring dbt_project.yml

* [post](https://discourse.getdbt.com/t/analyzing-fishtowns-dbt-project-performance-with-artifacts/2214) on performance tracking but from dbt side not query history side

#### Documentation on code setup

* quick-start [dbt core bigquery](https://docs.getdbt.com/guides/manual-install?step=1)

* documentation on [bigquery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) with dbt

* dbt [schema creation](https://docs.getdbt.com/docs/build/custom-schemas) documentation

* documentation on [incremental models](https://docs.getdbt.com/docs/build/incremental-models)

* doc explaining [jinja](https://docs.getdbt.com/docs/build/jinja-macros) and best practices to apply it 

* run_query macro [documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/run_query)

* about [dbt compile](https://docs.getdbt.com/reference/commands/compile)

* [graph contect variables](https://docs.getdbt.com/reference/dbt-jinja-functions/graph#accessing-models), helpful to build cleanup bigquery env macro

* [run start timestamp](https://docs.getdbt.com/reference/dbt-jinja-functions/run_started_at) brief explanation

* [configuring incremental models](https://docs.getdbt.com/docs/build/incremental-models) doc

* [hooks and operations](https://docs.getdbt.com/docs/build/hooks-operations)

* [pre/post hooks](https://docs.getdbt.com/reference/project-configs/on-run-start-on-run-end) to implement additional configurations apart from model execution (grant, alter table etc)

### Good to know 3rd party DBT links

* [medium article](https://blog.det.life/5-useful-loop-patterns-in-dbt-f1d959ab38b9) on implementing for-loops with jinja

* interesting approach to possibly implement RLS in dbt in this [medium article](https://medium.com/@azart0308/dbt-dynamic-column-selection-macros-4df5faaee42d) 

* [stackoverflow post](https://stackoverflow.com/questions/73157834/change-column-name-dynamically-using-mapping-table-dbt) helped develop for-loop column name concept

* [jinja cheat sheet](https://datacoves.com/post/dbt-jinja-cheat-sheet)

* [macro use-cases](https://www.getorchestra.io/guides/best-dbt-core-macros-examples-and-use-cases)

* [building macros](https://foundinblank.hashnode.dev/unlock-advanced-dbt-use-cases-with-the-meta-config-and-the-graph-variable) guide, helpful hints how to leverage python funcs

### Helpful links about the NYC taxi dataset

* where [data documentation](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) background can be found

* background on the logic behind [ratecode](https://www.nyc.gov/site/tlc/passengers/taxi-fare.page).

* [article](https://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/#late-night-taxi-index) on analysis about taxi trips in relation to time, social life etc 

### Dataset cleanup

* taxidataset [data cleaning](https://medium.com/@linniartan/nyc-taxi-data-analysis-part-1-clean-and-transform-data-in-bigquery-2cb1142c6b8b), which also includes how to convert geospatial coordinates to zone_id

* cleanup trips [medium](https://medium.com/@muhammadaris10/nyc-taxi-trip-data-analysis-45ecfdcb6f91) article

* cleanup tips and analysis [mdeium article](https://medium.com/@haonanzhong/new-york-city-taxi-data-analysis-286e08b174a1)

* determine if date is [public holiday](https://unytics.io/bigfunctions/bigfunctions/is_public_holiday/#examples)

* round timestamp to nearest [15min](https://stackoverflow.com/questions/53028983/round-timstamp-to-nearest-15-mins-interval-in-bigquery)

* good [YT video](https://www.youtube.com/watch?v=iz6lxi9BczA) on Bigquery query optimization

### Bigquery docs

* query cahce [docs](https://cloud.google.com/bigquery/docs/cached-results#cache-exceptions)

* this [section of documentation](https://cloud.google.com/bigquery/docs/best-practices-performance-overview) is great to understand query processing and computing to then optimize queries accordingly

* [article](https://datacoves.com/post/dbt-test-options) about different types of testing to implement