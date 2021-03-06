## Description
- The data pipeline ingests and transforms a movies data set in JSON format from an external repository (S3, HDFS, etc), persisting in Parquet on the same repository.
- One version of the data set per execution date is preserved.
- The staging area (where data is ingested directly from the repository) is transient (overwritten on every execution).

## Running instructions
1. Checkout the repository on a machine where Docker is available.
2. Run `docker-compose up -d` to spin up the container.
3. Run `docker exec -it movies_etl bash` to get into the container. 
4. Run `vim config.yaml` to edit it as needed: specify the data repository (default is local filesystem). If it's S3, AWS credentials need to be provided in the environment. 
5. Once there, the following make commands are available:
    1. `make test`: run the unit tests.
    2. `make run`: run the application. Must specify `task` argument, e.g. `make run task=ingest` or make `run task=transform`.  
       Feel free to also edit the `spark-submit` arguments on the `makefile`.
