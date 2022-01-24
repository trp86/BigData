"""This module is responsible for transform (T) in ETL.This module only contains functions very specific \
to transformation of trip data."""

from pyspark.sql import DataFrame, SparkSession, functions as f
from src.jobs import transform
from pyspark.sql.functions import lit


def perform_dq_and_add_additional_columns(
    df: DataFrame, config_dict: dict, sparksession: SparkSession
) -> tuple:
    """Columns undergo data quality check and additional columns are added."""
    # Create empty dataframe with schema present in input dataframe along with additional column rejectReason
    error_empty_df: DataFrame = sparksession.createDataFrame(
        sparksession.sparkContext.emptyRDD(), df.schema
    ).withColumn("rejectreason", lit(""))

    # Data Quality check for columns where negative values are not allowed
    trip_data_negative_check_columns = config_dict["trip.metadata"][
        "dq.negativevaluecheck.columns"
    ].split(",")
    (
        success_df_negative_value_check,
        error_df_negative_value_check,
    ) = transform.filter_records_having_negative_value(
        df=df,
        column_names=trip_data_negative_check_columns,
        error_empty_df=error_empty_df,
    )

    # Data Quality check for invalid datetime values
    trip_data_datetimestamp_check_columns = config_dict["trip.metadata"][
        "dq.datetimestampformatcheck.columns"
    ].split(",")
    (
        success_df_datetime_check,
        error_df_datetime_check,
    ) = transform.filter_records_having_improper_datetime_value(
        df=success_df_negative_value_check,
        column_names=trip_data_datetimestamp_check_columns,
        error_empty_df=error_empty_df,
    )

    # Typecast columns for trip data
    trip_data_column_details = list(
        map(lambda x: x.split(":"), config_dict["trip.metadata"]["columns"].split("|"))
    )
    list(
        map(
            lambda a: a == a.insert(2, "") if len(a) == 2 else a,
            trip_data_column_details,
        )
    )
    trip_data_typecasted = transform.typecastcolumns(
        success_df_datetime_check, trip_data_column_details
    )

    # Data Quality check for comparing two columns or value
    trip_data_columnsorvalue_check_columns = config_dict["trip.metadata"][
        "dq.columnsorvalue.compare"
    ].split("|")
    (
        success_df_columnsorvalue_check,
        error_df_columnsorvalue_check,
    ) = transform.df_columns_compare(
        df=trip_data_typecasted,
        compare_expressions=trip_data_columnsorvalue_check_columns,
        error_empty_df=error_empty_df,
    )

    # Add additional columns
    success_df = add_trip_day_of_week_column(
        add_trip_hour_column(add_trip_date_column(success_df_columnsorvalue_check))
    )
    error_df = error_df_negative_value_check.union(error_df_datetime_check).union(
        error_df_columnsorvalue_check
    )

    return (success_df, error_df)


def add_trip_date_column(df: DataFrame) -> DataFrame:
    """
    Column trip_date is added to the dataframe if pickup_datetime column exists.

    Args:
        df (DataFrame): Spark DataFrame to which trip_date column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    transform.check_if_column_exists_in_df(df, ["pickup_datetime"])

    return df.withColumn("trip_date", f.to_date(f.col("pickup_datetime"), "MM/dd/yyyy"))


def add_trip_hour_column(df: DataFrame) -> DataFrame:
    """
    Column trip_hour is added to the dataframe if pickup_datetime column exists.

    Args:
        df (DataFrame): Spark DataFrame to which trip_hour column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    transform.check_if_column_exists_in_df(df, ["pickup_datetime"])

    return df.withColumn("trip_hour", f.hour(f.col("pickup_datetime")))


def add_trip_day_of_week_column(df: DataFrame) -> DataFrame:
    """
    Column trip_day_of_week is added to the dataframe if pickup_datetime column exists.

    Args:
        df (DataFrame): Spark DataFrame to which trip_day_of_week column needs to be added
    """
    # Check if column exists in dataframe. If not then raise error
    transform.check_if_column_exists_in_df(df, ["pickup_datetime"])

    return df.withColumn("trip_day_of_week", f.dayofweek(f.col("pickup_datetime")))
