### Helpful query syntax

* getting row count and byte size: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.__TABLES__`

* get qhistory: `region-{REGION_CODE_2}.INFORMATION_SCHEMA.JOBS_BY_USER`

* get tables and column names: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.COLUMNS`

* get table details like table type and the table DDL: `${PROJECT_ID}.schemaName.INFORMATION_SCHEMA.TABLES`

* bytes to GB: bytes/powe(10,9)

* bytes to TB: bytes/pow(10,12)

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