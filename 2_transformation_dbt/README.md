## Background

This part of the project is to fullfill the Transformation (T) of ELT of the NYC taxi data into Bigquery. `Data Build Tool (DBT)` was emplyed for this. This was done to explore the advantages of the tool, enabling data modeling, DAG orchestration and testing using a python/jinja framework. 

## Setup

- development was carried out exclusively in a docker container to verify development would not come across package version conflicts.

- for working with docker, `devcontainer` extension in `VS Code` was employed, details on how this was carried out can be found in [setup](setup.md)

## Transformation Overview

- The 4 step/layer approach was taken here 

    + **clean**: The goal here was to collate all 67 original tables from `nytaxi_raw` schema to 4 trip type tables. This was done updating entries and adjusting them accordingly if they were historically reported differently and updating data type accordingly. This possible via custom [macros](macros), `update_*`. At the conclusion of this layer, the tables were materialized in `nytaxi_clean` schema.

    + **stage**: The goal of this layer was to identify potential faulty records and filter them out of the pipeline. This was done in 2-stage approach via views in [stage](models/stage): `__1a_id_duplicate_records` and `__1b_id_faulty_trips`. At the conclusion of this layer, the views were stored in `nytaxi_stage` schema.

    + **core**: The goal of this layer was to create, as it is termed in industry, "single source of truth". The views in stage (which filters out faulty records) are materialized into `_fact_trips` tables here. At the conclusion of this layer the tables were materialized in `nytaxi_core` schema.

    + **mart**: The goal of this layer is to calculate metrics to dimension tables: `dm_daily_stats` and `dm_monthly_stats`, with all the trip types together. At the conclusion of this layer, the tables were materialized in `nytaxi_mart` schema. 

- Two types of transformations were applied 
    
    + core1/mart1: this approach is where the terms/dimensions that were represented in the original raw data as codes/IDs (location_id, ratecode, payment_type etc.) were translated to end-user friendly terms right at the core transformation layer. It is hypothesized that this will be more compultationally expensive since record changes occur at the entity layer. 

    + core2/mart2: this approach is where the terms/dimensions that were represented in the original raw data as codes/IDs (location_id, ratecode, payment_type etc.) were translated to end-user friendly terms **after** metrics were calculated in mart layer. It is hypothesized that this approach will be more efficient as there are less records will experience changes. It is also hypothesized that this approach is more practical since the entities are probably not of interest to the end-user, therefor only applying computationally expensive changes where needed. 

**DAG plan DBT compiled solely based on the model dependencies**

![dbt-dag](../images/2_transformation_dbt/dbt-dag.png)

## Performance testing 

This was carried out using envrionment cleanup macros and `dbt build` commands.

**Full details can be found in**  👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉 [testing_protocol](testing_protocol.md)

## 🚀 Highlights 🚀

1. Made testing/development easier

    + variation of data volume test and methods were made possible by enabling/disabling cetrain models via `+enabled` parameter in `dbt_project.yml`

    + custom creating macros to cleanup BigQuery environment: [copy_clone_raw_tables](macros/copy_clone_raw_tables.sql) and [clean_bigquery_env](macros/clean_bigqeury_env.sql). Without these macros, would have to had manually dropped and copy cloned needed tables. Instead, based on input parameters, these macros copy cloned what was needed and dropped tables in Bigquery that were no longer modeled in the project, aka hanging tables. 

2. `DRY` (dont repeat yourself) coding 

    + again this is where macros came in handy. 
    
    + Syntax for case statement for transforming records were used multiple times across the different models. It was possible to create each type of case statement 1x via a macro and implement them across model however many times as needed.

    + also, it was easier to make the case statements more generic to capture edge cases from all trip types 👉👉👉 DRY

3. jinja incorporation made pipeline compilation seamless 

    + the `{{ ref() }}` and `{{ source() }}` jija syntax indicated to DBT the pipeline dependencies. With this, DBT was able to orchestrate the DAGs correctly independently, relieving the user of another configuration to manage during development. 

    + historically this configuration is done manually with any given orchestrator (airflow, step functions etc). When a data model grows larger and is under going extensive development and testing, this task can become incredibly tedious and very error prone.

    + With DBT + jinja taking care of this under the hood, more time is invested in data model development and no errors arise due to transformation dependency issues.

4. testing and constraints 

    + testing in DBT is a powerful capability. There a wide range of tests that can be applied (out of the box or customized), how the results can impact that transformatin pipeline (error or warn) and it acting as a type of QA to help the user improve the data model. 

    + like in pt.3, their dags are also facilitated by DBT under the hood, so no manual orchestration coordination is necessary

    + a really beneficial feature is constraints in the `schema.yml` of each file. In the schema, each model can be defined with a description, the dimensions (description and respective data types). When implementing the syntax:

        ```
        config:
            contract:
                enforced: true
        ```

    + data types define in the schema must be true in the target database (BigQuery in this case) as well. Should these "contract" not be true, the model compilation fails. This may seem quite tedious at first, but data contracts and restrictions lead to better documnentation, data governance and data qualtity

5. documnetation compilation 

    + with successful model compilation and `schema.yml`, rendering project documentation is quite easy and well formated. This is beneficial in collaborative work where other end users will then use the data model for further data interrogations, reporting etc. 

6. hooks

    + dbt permits for additional sql/python commands to be executed apart from those defined in `models`

    + this is most beneficial when needing to apply grants to different roles after a table is created/re-created

    + this can be executed via post hooks (after each model compilation or at the end of each run). DBT documentation is quite extensive on what these hooks can do and what can be implemented in them.

## 🫣 🫠 Limitations 😵‍💫 🤐

* I would say the greatest short coming I see at the moment is mostly in how dbt builds the models and incremental loading configurations 

* DBT by default always `create or replace` tables as opposed to `create` + `insert into`. From working with DWH developers, the best practice is to create tables with specific column names and fized data types (as opposed to create as select) and when doing delta/incremental loading, to do truncate/delete from then insert new records. Looking semi extensively into the documentation, the `delete+insert` incremental method is only available out of the box in other datawarehouses like snowflake, but not for BigQuery.

* there is the possibility to customize incremental method, but that is more for advanced users. This can be a goal for future projects 😎