# spark-movies-etl
- The data pipeline ingests and transforms a movies dataset:
  - The first task ingests the dataset from the `raw` bucket (json) into the `standardised` one (parquet).
  - A subsequent task consumes the dataset from `standardised`, performs transformations and business logic, and persists into `curated`.

## Running instructions
1. Clone the repository on a machine where Docker is available.
2. Run `docker-compose up -d` to spin up the container.
3. Run `docker exec -it movies_etl bash` to get into the container. 
4. Run `vim config.yaml` to edit it as needed: specify the data repository (default is local filesystem). If it's S3, AWS credentials need to be provided in the environment. 
5. Once there, the following make commands are available:
    1. `make test`: run the unit tests.
    2. `make run`: run the application. Must specify `task` argument, e.g. `make run task=ingest` or make `run task=transform`.  
       Feel free to also edit the `spark-submit` arguments on the `makefile`.
