"""This module is responsible for transform (T) in ETL."""

from pyspark.sql import DataFrame, functions as func
from pyspark.sql.functions import *
import functools
from pyspark.sql.types import *


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

def typecastcolumns(df: DataFrame, columns_with_data_type_details: list) -> DataFrame:

    """
    Typecasts the spark dataframe as per the data type and precesion if any

    Args:
        df (DataFrame): Spark DataFrame whose column is to be renamed
        columns_with_data_type_details (List[(str, str, str)]): List of tuples. Tuple have 3 elements. 0 -> Column Name, 1-> Column DataType, 2->Column Precision 
    """

    def typecast(df: DataFrame, column_detail: list)-> DataFrame:
    
        data_types_dict = {'int' : IntegerType(), 'double': DoubleType(), 'string' : StringType(), 'datetime' : TimestampType(), 'decimal': DecimalType() }
        typecasted_df = None

        if column_detail[1]  == "string" or column_detail[1]  == "int" or column_detail[1]  == "double" or column_detail[1]  == "datetime":
            typecasted_df = df.withColumn(column_detail[0], col(column_detail[0]).cast(data_types_dict.get(column_detail[1])))
        elif column_detail[1]  == "decimal":
            typecasted_df = df.withColumn(column_detail[0], col(column_detail[0]).cast(column_detail[1] + column_detail[2]))
        elif column_detail[1]  == "date":
            ## for date converion got issue and URL: https://stackoverflow.com/questions/62943941/to-date-fails-to-parse-date-in-spark-3-0
            ## and https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html helped to fix it.
            typecasted_df = df.withColumn(column_detail[0], to_date(col(column_detail[0]), column_detail[2]))
        else :
            # Raise exception if wrong data type is provided
            raise Exception("Unsupported data type for typecasting " + column_detail[1] )


        return typecasted_df

    return functools.reduce(
        # function
        lambda accumulated_df, column_detail : typecast(accumulated_df, column_detail), 
        # list
        columns_with_data_type_details,
        # accumulator
        df
        )

    
def filter_records_having_negative_value(df: DataFrame, column_names: list, error_empty_df: DataFrame) -> tuple:
    """
    Filter out the records where negative values are not accepted and create an error dataframe with a reason

    Args:
        df (DataFrame): Spark DataFrame whose column is to be renamed
        column_names (list): Columns where negative value check will be performed on spark dataframe
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, column_names)

    # Error DataFrame
    error_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, column_name : accumulated_df.union(df.filter(col(column_name) < 0 ).withColumn("rejectreason", lit(column_name + " is negative"))),
        # list
        column_names,
        # accumulator
        error_empty_df
    ) 

    # Success DataFrame
    success_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, column_name : accumulated_df.filter(col(column_name) >= 0 ),
        # list
        column_names,
        # accumulator
        df
    ) 

    return (success_df, error_df)
    

def filter_records_having_improper_datetime_value(df: DataFrame, column_names: list, error_empty_df: DataFrame) -> tuple:
    """
    Filter out the records where datetime value is improper
    Args:
        df (DataFrame): Spark DataFrame whose column is to be renamed
        column_names (list): Columns where datetime value check will be performed on spark dataframe
    """
    # Check if column exists in dataframe. If not then raise error
    check_if_column_exists_in_df (df, column_names)
    
    # Error DataFrame
    error_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, column_name : accumulated_df.
        union(
            df.withColumn("timestamp_typecasted" , to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss")).filter(col("timestamp_typecasted").isNull()).drop(col("timestamp_typecasted")).withColumn("rejectreason" , lit(column_name + " datetime format is incorrect"))
        ),
        # list
        column_names,
        # accumulator
        error_empty_df
    ) 

    # Success DataFrame
    success_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, column_name : accumulated_df.
        withColumn("timestamp_typecasted" , to_timestamp(col(column_name), "yyyy-MM-dd HH:mm:ss")).
        filter(col("timestamp_typecasted").isNotNull()).
        drop(col("timestamp_typecasted")),
        # list
        column_names,
        # accumulator
        df
    )

    return (success_df, error_df) 

def df_columns_compare(df: DataFrame, compare_expressions: list, error_empty_df: DataFrame) -> tuple:

    def column_compare(df: DataFrame, compare_expr: str, success_or_error: bool) -> DataFrame:
        compare_expr_list = compare_expr.split(""" """)
        (col_name, compare_operator, value_or_col_tobe_compared) = (compare_expr_list[0].strip(), compare_expr_list[1].strip(), compare_expr_list[2].strip())

        # Get the column datatype
        col_data_type = df.select(col_name).dtypes [0] [1]

        # For String datatypes compare operator can be only (=, !=)
        if (col_data_type == "string" and (compare_operator != "=" or compare_operator != "!=" )):
            raise Exception("For string datatype compare operator could only be = and !=")

        # should throw exception if comparision operator is apart from =,!=,>,<,>=,<=
        if not (compare_operator == "=" or compare_operator == "!=" or compare_operator == ">" or compare_operator == "<" or compare_operator == ">=" or compare_operator == "<="):
            raise Exception("Invalid comparison operator!!!!")     

        compare_expr = col_name.strip() + " " + compare_operator.strip() + " " + value_or_col_tobe_compared.strip()
        return df.filter(expr(compare_expr)) if success_or_error else df.filter(~ expr(compare_expr)).withColumn("rejectreason", lit(col_name + " is not " + compare_operator + " " + value_or_col_tobe_compared))
            
    # Error dataframe
    error_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, compare_expr : accumulated_df.union(column_compare(df, compare_expr, bool(False))),
        # list
        compare_expressions,
        # initial or start point 
        error_empty_df
    )
    #convert all columns of error dataframe to string
    error_df = error_df.select([error_df[c].cast('string').alias(c) for c in error_df.columns])
    
    # Success dataframe
    success_df: DataFrame = functools.reduce(
        # function
        lambda accumulated_df, compare_expr : column_compare(accumulated_df, compare_expr, bool(True)),
        # list
        compare_expressions,
        # initial or start point 
        df
    ) 
     
    return (success_df, error_df)

def replace_T_with_negligible_values(df: DataFrame, columns_with_data_type_details: list) -> DataFrame:

    """
    Typecasts the spark dataframe as per the data type and precesion if any

    Args:
        df (DataFrame): Spark DataFrame whose T is to be replaced with negligible values
        columns_with_data_type_details (List[(str, str, str)]): List of tuples. Tuple have 3 elements. 0 -> Column Name, 1-> Column DataType, 2->Column Precision 
    """

    def replace_T(df: DataFrame, column_detail: tuple) -> DataFrame:
        col_name = column_detail[0]
        col_data_type = column_detail[1]

        if col_data_type != "decimal" :
            return df 

        # Check if column exists in dataframe. If not then raise error
        check_if_column_exists_in_df (df, [col_name])

        return df.withColumn(col_name, when(col(col_name) == "T", "0.0001")
                                       .otherwise(col(col_name)))

    return functools.reduce(
        # function
        lambda accumulated_df, column_detail : replace_T(accumulated_df, column_detail),
        # list
        columns_with_data_type_details,
        # initial or start point 
        df
    )    

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
