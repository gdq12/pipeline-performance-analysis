## Background

This project was created to explore ELT methods and optimizations within the data processing approach using tools: [Dataproc](https://cloud.google.com/dataproc?hl=en), [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), [Mage.Ai](https://www.mage.ai/) and [DBT](https://www.getdbt.com/). Inspiration is based on lessons learned from DataTalks Club [dataengineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). The [nyc taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) was used for development for its volume and easy accessibility. 

## Project Overview 

![pipeline diagram](images/project/pipeline_diagram.jpg)

## 🚀 Highlights 🚀

### Extract-Load

- Used [GCPs Dataproc Hadoop/Spark Clusters](https://cloud.google.com/dataproc?hl=en)

- Working with spark clusters made it much easier to extract and load the parquet data to BigQuery. **850 GB large** 😱. This would of not been possible using memory intensive libraries such as pandas.

- since this employs spark clusters and the environment infrastructure is configured by GCP instead, it was an easier solution to employ.

- All ride data, `2009 - 2024` for all trip types (`yellow`, `green`, `fhv`, `fhvhv`), were successfully loaded into GCP `cloud storage` and `BigQuery`. 

- `DRY` method was succesully employed.

**Want to find out more? Checkout details here** 👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉 [1_extract_load_dataproc](1_extract_load_dataproc)

### Transformation

- Used `DBT` for model development and DAG orchestration 

- developement and deployment were made posible due to: `macros`, `jinja`, `dbt tests`, `data contracts/constraints`

- `DRY` coding was made possible in particular due to `macros`

- performance was more feasible and easier to document with DBT, it permitted proper cleanup/setup of raw environment and very feasible to analyze the quantified results. This was possible due to `macros` and `incremental` loading

**Want to find out more? Checkout details here** 👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉👉 [2_transformation_dbt](2_transformation_dbt)