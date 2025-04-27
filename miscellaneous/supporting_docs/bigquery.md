### Helpful query syntax

* getting row count and byte size: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.__TABLES__`

* get qhistory: `region-{REGION_CODE_2}.INFORMATION_SCHEMA.JOBS_BY_USER`

* get an idea which query jobs are using shuffle capacity extensively by taking a look at the `period_shuffle_ram_usage_ratio` column in: `region-{REGION_CODE_2}.INFORMATION_SCHEMA.JOBS_TIMELINE`

* get tables and column names: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.COLUMNS`

* get table details like table type and the table DDL: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.TABLES`

* bytes to GB: bytes/powe(10,9)

* bytes to TB: bytes/pow(10,12)

## Notes on query performance 

what to look out for in qhistory analysis: 

* queryplan and timeline statistics help understand if some stage dominate resource utilization 
    - example join produce more rows than it was inputted means there is something possibly wrong with the join

* significant different between avg vs max time spent by workers in a stage can indicate data skew (1 worker processing more data than the others)

* if a lot of time is spent on CPU tasks, then perhaps consider approx funcs which are 1% deviation only 

* Slot contention occurs when your query has many tasks ready to start executing, but BigQuery can't get enough available slots to execute them.
    - best to either reduce data for operation or increase slot quotas 

* when a stage is complete, it stores the intermediaete results in shuffle (sounds like temp memory). When too much data needs to be written into shuffle, it reaches capacity and get insuffiecient shuffle memory error 
    - try to reduce data needed for the given operation 

* partition skew: Skewed data distribution can cause queries to run slowly. When a query is executed, BigQuery splits data into small partitions. You can't share partitions between slots. Therefore, if the data is unevenly distributed, some partitions become very large, which crashes the slot that processes the oversized partition. Skew occurs in JOIN stages. When you run a JOIN operation, BigQuery splits the data on the right side and left side of the JOIN operation into partitions. If a partition is too large, the data is rebalanced by repartition stages. If the skew is too bad and BigQuery cannot rebalance further, a partition skew insight is added to the 'JOIN' stage. This process is known as repartition stages. If BigQuery detects any large partitions that cannot be split further, a partition skew insight is added to the JOIN stage.

* review run time stats:
    - wait ms longer than when query previously ran --> see if not have enough slot available, or load-balance resources 
    - check wait ms for a given stage and see what went on in the previous stage, substantial DDL change can cause this 
    - check shuffle output bytes when comparing to previous run or to the previous executed stage within the same run. joining on non-unique keys can cause high cardinality joins 

* for a given query, examine the duration and processing volume and compare those stats to the pre and post stage. see if there are any stats that seem off or not as expected 

### Usefule BigQuery functions

* determine if date is [public holiday](https://unytics.io/bigfunctions/bigfunctions/is_public_holiday/#examples)

* round timestamp to nearest [15min](https://stackoverflow.com/questions/53028983/round-timstamp-to-nearest-15-mins-interval-in-bigquery)


### Docs for working with BigQuery

* good [YT video](https://www.youtube.com/watch?v=iz6lxi9BczA) on Bigquery query optimization

* query cahce [docs](https://cloud.google.com/bigquery/docs/cached-results#cache-exceptions)

* this [section of documentation](https://cloud.google.com/bigquery/docs/best-practices-performance-overview) is great to understand query processing and computing to then optimize queries accordingly

* intro doc to [query optimization](https://cloud.google.com/bigquery/docs/best-practices-performance-overview) --> best to look at consecutive docs in this series, quite informative 

* [query insights](https://cloud.google.com/bigquery/docs/query-insights) provide explanation on how to examine execution graph and how to interpret

* [query plan](https://cloud.google.com/bigquery/docs/query-plan-explanation#query_plan_information), some hepful info on what some cols are in the query history tbl 

* [YT overview video](https://www.youtube.com/watch?v=q9npE47O2UI) on how Bigquery processes queries 

* [job query history doc][https://cloud.google.com/bigquery/docs/information-schema-jobs-by-user], good starter doc to understand the dimensions in qhistory table 

* [create session](https://cloud.google.com/bigquery/docs/sessions-create) so can run temp table queries