"""Entry point to the pyspark job."""
import typer
from pathlib import Path
from src.jobs.main import jobs_main, spark_build
from src.jobs.utils.general import EnvEnum
from src.jobs.utils import log_utils


def main(
    env: EnvEnum = typer.Argument(..., help="Environment for the spark-job"),
    # file_path: Path where all the config files are present
    config_file_path: str = typer.Argument(
        f"{Path(__file__).parent}/src/resources/", help="Path where all the config files are present"
    ),
) -> None:
    """Execute main function for the package."""
    with spark_build(env=env) as sparksession:
        logger = log_utils.Logger(env=env, spark=sparksession)
        logger.info("Spark and logger initialized")
        jobs_main(sparksession, logger, config_file_path=config_file_path) 


if __name__ == "__main__":
    typer.run(main)
