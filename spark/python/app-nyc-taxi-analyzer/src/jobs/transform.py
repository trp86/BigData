"""This module is responsible for transform (T) in ETL."""

from pyspark.sql import DataFrame, functions as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
import functools

def check_if_column_exists_in_df(df: DataFrame, column_list_to_be_checked: list) -> None:

    """
    Check if column exists in spark dataframe.

    Args:
        df (DataFrame): Spark DataFrame to  check if column exists
        column_list_to_be_checked (list): List of column names to be checked   
    
    """
    columns_present_in_df = df.columns
    columns_not_present_in_df = list(set(column_list_to_be_checked) - set(columns_present_in_df))

    if len(columns_not_present_in_df) > 0 :
        raise IOError('Columns not present in dataframe::- ' + str(columns_not_present_in_df))
    

def rename_column_in_df(df: DataFrame, old_column_name: str, new_column_name: str) -> DataFrame:
    """
    Rename a column if exists in spark dataframe

    Args:
        df (DataFrame): Spark DataFrame whose column is to be renamed
        old_column_name (str): Old Column Name
        new_column_name (str): New Column Name
    """
    # Check if column exists in dataframe else raise exception
    check_if_column_exists_in_df(df, [old_column_name])

    # Rename the column in dataframe
    return df.withColumnRenamed(old_column_name, new_column_name)

def filter_records_having_negative_value(sparksession: SparkSession, df: DataFrame, column_names: list) -> tuple:
    """
    Filter out the records where negative values are not accepted and create an error dataframe with a reason

    Args:
        df (DataFrame): Spark DataFrame whose column is to be renamed
        column_names (list): Columns where negative value check will be performed on spark dataframe
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, column_names)

    # Create empty dataframe with schema present in input dataframe along with additional column rejectReason
    empty_df: DataFrame = sparksession.createDataFrame(sparksession.sparkContext.emptyRDD(), df.schema).withColumn("rejectreason" , lit(""))

    error_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, column_name : accumulated_df.union(df.filter(col(column_name) < 0 ).withColumn("rejectreason", lit(column_name + " is negative"))),
        # list
        column_names,
        # accumulator
        empty_df
    ) 

    print("PRINT::::::")
    error_df.show(10, truncate = bool(False))
    print("PRINT COMPLETE::::::")
    return (df, df)
    



##########
def explode_df(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Explodes the input_column.

    Args:
        df (DataFrame): DataFrame which contains a column "input_column"
        input_col (str): input column name
        output_col (str): output column name

    Returns:
        DataFrame with column exploded

    """
    return df.select(
        func.explode(func.split(func.col(input_col), " ")).alias(output_col)
    )


def clean_df(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Clean the df's column by removing non-alphanumeric \
    characters from the column and empty-strings.

    Args:
        df (DataFrame): DataFrame which contains a column "input_column"
        input_col(str): input column for the transformation
        output_col(str): output column for the transformation

    Returns:
        DataFrame with cleaned data

    """
    return df.select(
        func.regexp_replace(func.col(input_col), r"[^a-zA-Z\d]", "").alias(output_col)
    ).where(func.col(output_col) != "")


def lower_case_df(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Lower cases a DataFrame's column.

    Args:
        df (DataFrame): DataFrame whose column needs to be lower cased
        input_col (str): input column for the transformation
        output_col (str): output column for the transformation

    Returns:
        DataFrame which contains the lower cased column
    """
    return df.select(func.lower(func.col(input_col)).alias(output_col))


def count_df(df: DataFrame, input_col: str, output_col: str) -> DataFrame:
    """
    Count the instances of the input_column and enters them in output_column.

    Args:
        df (DataFrame): DataFrame whose column needs to be counted
        input_col (str): input column name which should be counted
        output_col (str): output column name containing the count

    Returns:
        DataFrame which contains the count of words

    """
    return df.groupBy(input_col).agg(func.count(input_col).alias(output_col))


def transform_df(raw_df: DataFrame) -> DataFrame:
    """
    Count the number of occurrence of words in a single-column raw dataframe.

    Args:
        raw_df (DataFrame): raw dataframe extracted from the text

    Returns:
        DataFrame of single-column text file

    """
    return (
        raw_df.transform(
            lambda df: explode_df(df, input_col="value", output_col="exploded")
        )
        .transform(lambda df: clean_df(df, input_col="exploded", output_col="cleaned"))
        .transform(
            lambda df: lower_case_df(df, input_col="cleaned", output_col="lower_cased")
        )
        .transform(lambda df: count_df(df, input_col="lower_cased", output_col="count"))
    )
