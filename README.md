## Background

This project was created to explore ELT methods and optimizations within the data processing approach using tools: [Dataproc](https://cloud.google.com/dataproc?hl=en), [Mage.Ai](https://www.mage.ai/) and [DBT](https://www.getdbt.com/). Inspiration is based on lessons learned from DataTalks Club [dataengineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). The [nyc taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) was used for development for its volume and easy accessibility. 

## Project Overview 

![pipeline diagram](images/project/pipeline_diagram.jpg)

## Highlights

### Extract-Load

- Used [GCPs Dataproc Hadoop/Spark Clusters](https://cloud.google.com/dataproc?hl=en)

- full implementation documented in [1_extract_load_dataproc](1_extract_load_dataproc)

- since this employs spark clusters and the environment infrastructure is configured by GCP instead, it was an easier solution to employ.

- All ride data, `2009 - 2024` for all trip types (`yellow`, `green`, `fhv`, `fhvhv`), were successfully loaded into GCP `cloud storage` and `BigQuery`. 

- `DRY` method was succesully employed. What is left to implement is testing and scheduling, but it was determined that it is out of scope for this project at this time.

### Transformation