## Description
- The ETL ingests and transforms a movies data set in JSON format from S3, persisting in Parquet on the same bucket.
- One version of the data set per execution date is preserved.
- The staging directory (where the IngestDataTask loads the data directly from S3) is transient (overwritten on every execution).

## Running instructions
1. Checkout the repository on a machine where Docker is available.
2. Run **docker-compose up -d** to spin up the container.
3. Run **docker exec -it movies_etl bash** to get into the container. 
4. Run **vim config.yaml** to edit it (AWS credentials need to be specified). Feel free to tweak other options :).
5. Once there, the following make commands are available:
    1. **make test**: run the unit tests.
    2. **make run**: run the application. Accepts optional **task** argument.

**NOTE**: For the task argument, the values "ingest" and "transform" are accepted. If the argument is not specified, both tasks will be run.  

Examples:  
**make run task=ingest**: Run only the ingest task.  
**make run**: Run the full pipeline.
