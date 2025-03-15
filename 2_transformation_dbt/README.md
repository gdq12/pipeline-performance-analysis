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
    
### Good to know DBT commands

* testing dbt connection/installation

    ```
    dbt debug
    ```

### Helpful Links 

* installing with docker [page](https://docs.getdbt.com/docs/core/docker-install)

* installing dbt with [pip](https://docs.getdbt.com/docs/core/pip-install)

* where [dbt maintained docker image name](https://github.com/dbt-labs/dbt-bigquery/pkgs/container/dbt-bigquery) was sourced from 

* repo on the different dbt [adaptors](https://github.com/dbt-labs/dbt-adapters?tab=readme-ov-file)

* documentation on [bigquery setup](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) with dbt

* quick-start [dbt core bigquery](https://docs.getdbt.com/guides/manual-install?step=1)

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
