## Intro 

In order to carry out performance testing, the following steps were taken:

1. updating [dbt_project.yml](dbt_project.yml)

    + one of the core/mart versions had to always be disabled so only transformation path was executed at a time. This was done by updating the `+enabled` value to false when the target model was to be disabled for a run.

2. `run_tag` variable needed to be updated 

    + this was to label the queries run in the query history table: `query_history_extract_load_transform_project`

    + the updated value is then implemented in the [get_query_history](macros/get_query_history.sql) macros, which is a post-hook triggered at the end of the transformation run (`dbt run/dbt build`). This macros inserts all info on the queries run from `INFORMATION_SCHEMA.JOBS_BY_USER` into a project table in the monitoring schema for later analysis

3. prep the raw schema for a given incremental/full-refresh run 

    + for this, another macros was created: [copy_clone_raw_tables][macros/copy_clone_raw_tables.sql]

    + depending on the input variables, the macro either does a COMPLETE refresh by dropping all raw tables and schemas prior to copy cloning the desired tables from the raw schema backup, or simply adds more tables to the raw schema 

    + this step is done to simulate incremental loading, where in the real world the raw data would be made slowly available over time 

5. execute `dbt build` as default incremental or as `--full-refresh`

    + the next section details what run_tag was updated to and what commands were executed for the transformations 

    + each transformation type required 6 executions (see details below), to simulate gradual increase of raw schema data made available for transformation

    + a full-refresh was also run in the end of each one to compare resource consumption between incremental and full refresh.

## Protocol for performance testing 

### core1/mart1 testing 

```
run_tag: 'core1/mart1 - initial_tables - (1) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow_tripdata_2009-01|yellow_tripdata_2011-01|green_tripdata_2014-01|fhvhv_tripdata_2019-02|fhv_tripdata_2015-01", yr_str: ".*", method: "refresh_schema"}'
dbt build --full-refresh --vars 'is_test_run: false'

run_tag: 'core1/mart1 - incremental - (2) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow|green|fhvhv|fhv", yr_str: "2011|2015|2019", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core1/mart1 - incremental - (3) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "green|fhvhv|fhv", yr_str: "2018|2020", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core1/mart1 - incremental - (4) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow|green|fhvhv|fhv", yr_str: "2010|2014|2017|2021|2022|2023", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core1/mart1 - incremental - (5) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: ".*", yr_str: ".*", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core1/mart1 - full refresh - (6) - datasource'
dbt build --full-refresh --vars 'is_test_run: false'

```

### core2/mart2 testing

```
run_tag: 'core2/mart2 - initial_tables - (1) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow_tripdata_2009-01|yellow_tripdata_2011-01|green_tripdata_2014-01|fhvhv_tripdata_2019-02|fhv_tripdata_2015-01", yr_str: ".*", method: "refresh_schema"}'
dbt build --full-refresh --vars 'is_test_run: false'

run_tag: 'core2/mart2 - incremental - (2) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow|green|fhvhv|fhv", yr_str: "2011|2015|2019", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core2/mart2 - incremental - (3) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "green|fhvhv|fhv", yr_str: "2018|2020", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core2/mart2 - incremental - (4) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: "yellow|green|fhvhv|fhv", yr_str: "2010|2014|2017|2021|2022|2023", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core2/mart2 - incremental - (5) - datasource'
dbt run-operation copy_clone_raw_tables --args '{tbl_name_str: ".*", yr_str: ".*", method: "add_tables"}'
dbt build --vars 'is_test_run: false'

run_tag: 'core2/mart2 - full refresh - (6) - datasource'
dbt build --full-refresh --vars 'is_test_run: false'
```