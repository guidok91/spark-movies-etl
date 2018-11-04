## Considerations, decisions and caveats
- The ETL process supports execution multiple times a day, preserving the state of the movies at the time of each execution. Another option would have been to preserve one data set per day (deleting the data for the day before loading on each run).
- The staging table (where the IngestDataTask loads the data directly from S3) is transient (overwritten on every execution).
- A significant proportion of tests were developed for the application, although more should be created.

## Running instructions
1. Checkout the repository on a machine where Docker is available.
2. Run **docker-compose up -d** to spin up the container.
3. Run **docker exec -it movies_etl bash** to get into the container. 
4. Run **vim config.json** to edit it (AWS credentials need to be specified). Feel free to tweak other options :).
5. Once there, the following make commands are available:
    1. **make test**: run the unit tests.
    2. **make run_standalone**: run the application in standalone mode. Accepts optional **task** argument.
    3. **make run_yarn**: run the application in Yarn mode. Accepts **hadoop_conf_dir** argument and optional **task** argument. Additionally, "yarn" has to be specified as Spark master in **config.json**.

**NOTE**: For the task argument, the values "ingest" and "transform" are accepted. If the argument is not specified, both tasks will be run.  

Examples:  
**make run_standalone task=ingest**: Run only the ingest task in standalone mode.  
**make run_standalone**: Run the full pipeline in standalone mode.  
**make run_yarn hadoop_conf_dir=/path/to/a/dir** Run the full pipeline in yarn mode.  
