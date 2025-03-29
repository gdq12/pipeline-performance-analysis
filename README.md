## Background

This project was created to explore ELT methods and optimizations within the data processing approach using tools: [Dataproc](https://cloud.google.com/dataproc?hl=en), [Mage.Ai](https://www.mage.ai/) and [DBT](https://www.getdbt.com/). Inspiration is based on lessons learned from DataTalks Club [dataengineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). The [nyc taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) was used for development for its volume and easy accessibility. 

## Project Overview 

![pipeline diagram](images/project/pipeline_diagram.jpg)

### Extract-Load

Two types of approaches were executed for this part of the project:

* using [Mage.Ai](1_extract_load_mage)

    - Worked quite well in `DRY` method development, `testing` implementation, `scheduling` jobs and orchestration etc 

    - Current limitations faced were data volume imported into docker image. Defaukt version of this tool employs `pandas` for data loading and transformation. The data batches provided by the `TLC taxi website` were large parquet files that aren't ideal to be loaded into full memory.

    - Current requirement is update the docker image so it works with spark clusters so `pyspark` can be used instead.

* using [GCPs Dataproc Hadoop/Spark Clusters](1_extract_load_dataproc)

    - since this employs spark clusters and the environment infrastructure is configured by GCP instead, it was an easier solution to employ.

    - All ride data, `2009 - 2024` for all trip types (`yellow`, `green`, `fhv`, `fhvhv`), were successfully loaded into GCP `cloud storage` and `BigQuery`. 

    - `DRY` method was succesully employed. What is left to implement is testing and scheduling, but it was determined that it is out of scope for this project at this time.