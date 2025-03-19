Welcome to your new dbt project!
### Setup steps 

1. Build and run Docker image 

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

    + save json key to `~/Documents/` and `~/git_repos/pipeline-performance-analysis/2_transformation_dbt`
    
### Good to know DBT commands and more

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
    ```

* other

    + post `dbt compile --select "modelName"` can see the compiled sql script in folder: `target/projectName/models/schemaName/`

### Helpful Links 

* installing with docker [page](https://docs.getdbt.com/docs/core/docker-install)

* installing dbt with [pip](https://docs.getdbt.com/docs/core/pip-install)

* where [dbt maintained docker image name](https://github.com/dbt-labs/dbt-bigquery/pkgs/container/dbt-bigquery) was sourced from 

* repo on the different dbt [adaptors](https://github.com/dbt-labs/dbt-adapters?tab=readme-ov-file)

* documentation on [bigquery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) with dbt

* quick-start [dbt core bigquery](https://docs.getdbt.com/guides/manual-install?step=1)

* taxidataset [data cleaning](https://medium.com/@linniartan/nyc-taxi-data-analysis-part-1-clean-and-transform-data-in-bigquery-2cb1142c6b8b), which also includes how to convert geospatial coordinates to zone_id

* [dbt package hub](https://hub.getdbt.com/)

* [medium article](https://blog.det.life/5-useful-loop-patterns-in-dbt-f1d959ab38b9) on implementing for-loops with jinja

* interesting approach to possibly implement RLS in dbt in this [medium article](https://medium.com/@azart0308/dbt-dynamic-column-selection-macros-4df5faaee42d) 

* [stackoverflow post](https://stackoverflow.com/questions/73157834/change-column-name-dynamically-using-mapping-table-dbt) helped develop for-loop column name concept

* run_query macro [documentation](https://docs.getdbt.com/reference/dbt-jinja-functions/run_query)

* [jinja cheat sheet](https://datacoves.com/post/dbt-jinja-cheat-sheet)

* about [dbt compile](https://docs.getdbt.com/reference/commands/compile)

* [dbt-utls repo](https://github.com/dbt-labs/dbt-utils?tab=readme-ov-file#get_column_values-source), helpful on seeing how to implement macros into project

* [macro use-cases](https://www.getorchestra.io/guides/best-dbt-core-macros-examples-and-use-cases)

* good [post](https://discourse.getdbt.com/t/can-i-create-an-auto-incrementing-id-in-dbt/579/3) about auti-incremental ID col implementation in dbt

* dbt [schema creation](https://docs.getdbt.com/docs/build/custom-schemas) documentation

### Notes from the original README

1. Using the starter project

* Try running the following commands:

    - dbt run
    
    - dbt test

2. Resources:

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)

- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers

- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers

- Find [dbt events](https://events.getdbt.com) near you

- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
