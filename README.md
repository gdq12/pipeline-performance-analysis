## Background

This project was created to explore ELT methods and optimizations within the data processing approach using tools: [Dataproc](https://cloud.google.com/dataproc?hl=en), [PySpark](https://spark.apache.org/docs/latest/api/python/index.html), [DBT](https://www.getdbt.com/) and [BigQuery](https://cloud.google.com/bigquery/docs/introduction). Inspiration is based on lessons learned from DataTalks Club [dataengineering zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp). The [nyc taxi dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) was used for development for its volume and easy accessibility. 

## Project Overview 

![pipeline diagram](images/project/pipeline_diagram.jpg)

## ✨✨ Highlights ✨✨

### Extract-Load

- Able to load all `2009-2024` trip data (**850 GB** 😱) to BigQuery using [GCPs Dataproc Hadoop/Spark Clusters](https://cloud.google.com/dataproc?hl=en), in `DRY` code method. 

- All 4 trip types (`yellow`, `green`, `fhv`, `fhvhv`) were loaded using the same script, where only the input parameters for the `PySpark` script differentiated for each trip. There was also no need to pre-configure custom DDL statements for they were derived from the external/stage tables per parquet instead.

**Want to find out more? Checkout details here** 👉👉👉👉👉👉👉👉👉👉 [1_extract_load_dataproc](1_extract_load_dataproc)

### Transformation

- Benefits of using `DBT`: 

    + `DRY` coding in models with the help of `macros`

    + `dbt tests` and `data contracts/constraints` were instrumental in model development for improving data contracts, optimizing primary key creation and filtering out faulty data where detected 

    + `jinja`,  `post-run hooks` and `dbt run-operation macros` helped a great deal in collecting query performance stats and facilitating incremental testing for model method comparison

**Want to find out more? Checkout details here** 👉👉👉👉👉👉👉👉👉👉 [2_transformation_dbt](2_transformation_dbt)

### Insights 

#### Performance analysis 

- `DBT's` out of the box framework, plethora of community resources and BigQuery's rich query history stats enabled for a deep dive for model performance analysis 

- The analysis confirmed that **translating raw data identifiers is more efficient after metric aggregations are calculated**

- the analysis also revealed some current limitations in the better transformation approach that led to some slot contention, but with proposals for even better improvement and lessons learned for the future

**Want to find out more? Checkout details here** 👉👉👉👉👉👉👉👉👉👉 [3_insights/performance_testing](3_insights/performance_testing)

### 🔮 Future Outlook 🔮

- This framework and method can be used for other large data transformations and more extensive data modelling testing 

- Can further explore DBTs capacity by configuring custom incremental loading framework to determine if out of the box solutions are optimal or customization is best. It also depends on the type of loading. If its event driven data like streaming perhaps the out of the box solution is best, but for batch processing data it would be worth visiting other approaches 

- Since this is a batch centric data load (1 parquet file per trip type/reported month combo), incremental loading was determined by a given records parquet filename source (`datasource` dimension across the models). There was an interest to test out incremental loading by comparing `clone_dt` (when data is loaded from extract-load) vs `transformation_dt` (when a records was addded to the given model), but ran out of credits. It would be interesting in the future to test out this approach and see how it performs compared to the one currently implemented here

- Explore further possibilities in data quality testing via DBT. There is quite a bit of community support for testing with other libraries created by the community or even custom creating tests. This permits the possibility for accounting for new edge cases that arise over time when working with a given data set

- No tests were implemented in the extract-load portion of this project (in spark-dataproc), for the primary goal here was to work more with DBT this time around, but its a crucial component to implement in live production systems. This is something else to look further in the future

- Early in the project, an attempt was made to implement an orchestrator tool instead of Dataproc ([Mage](miscellaneous/1_extract_load_mage)), but the out of the box framework uses pandas as the data management library, which was not ideal for data size er parquet file for this project. The tool offered documentation on Spark integration, but at th time it was too cubersome to implement. This is another aspect to concentrate more in the future for these frameworks offer great assistance in DAG orchestration and test implementation for a more solid extract-load approach 