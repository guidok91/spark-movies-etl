# Movies data ETL (Spark)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/ci-cd-push.yml/badge.svg)

Spark data pipeline that processes movie ratings data.

## Data Architecture
We define a Data Lakehouse architecture with the following layers:
- `Raw`: Contains raw data directly ingested from an event stream, e.g. Kafka. This data should generally not be shared (can contain PII, duplicates, quality issues, etc).
- `Curated`: Contains transformed data according to business and data quality rules. This data can be shared and accessed as tables registered in a data catalog.

[Apache Iceberg](https://iceberg.apache.org/) is used as the table format for both the raw and curated layers.

<img width="1615" alt="image" src="https://github.com/user-attachments/assets/4c86e798-0cb2-4e43-b972-89a806e8a07a" />


## Data pipeline design
The Spark data pipeline:
- Reads data from the raw layer (`movie_ratings_raw` table) incrementally for a given date (filtering by `ingestion_date`).
- Performs data cleaning, transformations and business logic.
- Writes to the curated layer (`movie_ratings_curated` table) partitioned by `days(timestamp)` (leveraging [Iceberg's hidden partitioning](https://iceberg.apache.org/docs/latest/partitioning/) for optimal querying).

After persisting, Data Quality checks are run on the curated data using [Pandera](https://pandera.readthedocs.io/en/stable/pyspark_sql.html).

Note that for the purpose of running this project locally, we use an Iceberg catalog in the local file system.
In production, we could for instance use the AWS Glue data catalog, persisting data to S3. [See doc](https://iceberg.apache.org/docs/latest/aws/#spark).

Additionally, in a production scenario it's recommended to periodically run [Iceberg table maintenance operations](https://iceberg.apache.org/docs/latest/maintenance/).

## Packaging and dependency management
[uv](https://docs.astral.sh/uv) is used for Python packaging and dependency management.

Dependabot is configured to periodically upgrade repo dependencies. See [dependabot.yml](.github/dependabot.yml).

Since there are multiple ways of deploying and running Spark applications in production (Kubernetes, AWS EMR, Databricks, etc), this repo aims to be as agnostic and generic as possible. The application and its dependencies are built into a Docker image (see [Dockerfile](Dockerfile)).

In order to distribute code and dependencies across Spark executors [this method](https://spark.apache.org/docs/latest/api/python/tutorial/python_packaging.html#using-pyspark-native-features) is used.

## CI/CD
Github Actions workflows for CI/CD are defined [here](.github/workflows) and can be seen [here](https://github.com/guidok91/spark-movies-etl/actions).

The logic is as follows:
* On PR creation/update:
  * Run code checks and tests.
  * Build Docker image.
  * Publish Docker image to Github Container Registry with a tag referring to the PR, like `ghcr.io/guidok91/spark-movies-etl:pr-123`.
* On push to master:
  * Run code checks and tests.
  * Create Github release.
  * Build Docker image.
  * Publish Docker image to Github Container Registry with the latest tag, e.g. `ghcr.io/guidok91/spark-movies-etl:master`.

Docker images in the Github Container Registry can be found [here](https://github.com/guidok91/spark-movies-etl/pkgs/container/spark-movies-etl).

## Local development/execution instructions
A sample `movie_ratings_raw` table in a local Iceberg catalog ([data-lake-dev](data-lake-dev) directory) is included.

The repo includes a `Makefile`. Please run `make help` to see usage.

To execute the pipeline locally, run:
* `make setup` to install a local virtual env with the app and its dependencies.
* `make package` to compile the dependencies to be used in `spark-submit`.
* `EXECUTION_DATE=2021-01-01 make run-curate-data` to run the data transformation task.
* `EXECUTION_DATE=2021-01-01 make run-curate-data-quality-checks` to run the data quality checks on the output data.

Afterwards, we can run `make spark-sql-shell` to explore the data with SparkSQL, for example:
```sql
SELECT
    original_title,
    DATE_FORMAT(timestamp, 'yyyy-MM') AS month,
    ROUND(AVG(rating), 2) AS avg_rating
FROM
    movie_ratings_curated
WHERE
    original_title IN ('A Clockwork Orange', 'Scary Movie', 'Superbad')
GROUP BY
    ALL
ORDER BY
    original_title,
    month;
```
