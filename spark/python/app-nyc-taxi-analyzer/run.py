"""Entry point to the pyspark job."""
import typer
from pathlib import Path

from src.jobs.main import jobs_main, spark_build
from src.jobs.utils.general import EnvEnum, LibCommons
from src.jobs.utils import log_utils


def main(
    env: EnvEnum = typer.Argument(..., help="Environment for the spark-job"),
    file_path: str = typer.Argument(
        f"file://{Path(__file__).parent}/tests/resources/inputdata/yellow_tripdata_subset_2014-01.csv", help="File which will be parsed"
    ),
) -> None:
    """Execute main function for the package."""
    with spark_build(env=env) as spark:
        logger = log_utils.Logger(env=env, spark=spark)
        logger.info("Spark and logger initialized")

        libCommons = LibCommons(sparkSession=spark)
        jobs_main(spark, logger, file_path=file_path, libCommons=libCommons)


if __name__ == "__main__":
    typer.run(main)
