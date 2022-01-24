"""pytest entry into unittests."""
import pytest
from pyspark.sql.session import SparkSession


@pytest.fixture(scope="module")
def spark_session_test() -> SparkSession:
    """Create fixture for SparkSession."""
    return SparkSession.builder.appName("pytest").getOrCreate()
