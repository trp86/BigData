"""Testcases for transform.py file."""

from os import error, truncate
from src.jobs.utils.general import *
import pytest
from pyspark.sql import SparkSession
from pathlib import Path
from src.jobs import transform
from pyspark.sql.types import *
from pyspark.sql import Row 
import pandas as pd
from pyspark.sql.functions import col

@pytest.fixture
def init():
    app_name = "test_" + Path(__file__).parent.name
    test_spark_session = SparkSession.builder.appName(app_name).getOrCreate()
    libCommons = ""
    some_df = test_spark_session.createDataFrame([(1234,56.76), (2341,76.45)], schema='vendor_id string, total_amount string')
    column_details_string = "vendor_id:string|total_cust:int|fare:double|maximumtemperature:decimal:(14,4)|pickup_datetime:datetime|weather_date:date:d-M-y"
    some_column_details = list(map(lambda x: x.split(":"), column_details_string.split("|")))
    list(map(lambda a: a==a.insert(2,"") if len(a) == 2 else a , some_column_details))
    return libCommons, test_spark_session, some_df, some_column_details

@pytest.mark.check_if_column_exists_in_df
def test_column_list_to_be_checked_return_none_if_column_exist(init):
    # given
    some_df = init [2]
    column_list_to_be_checked = ["vendor_id", "total_amount"]
    
    # when
    actual = transform.check_if_column_exists_in_df(some_df, column_list_to_be_checked)
    
    # then
    assert actual is None


@pytest.mark.check_if_column_exists_in_df
def test_column_list_to_raise_error_if_column_not_exist(init):
    # given
    some_df = init [2]
    column_list_to_be_checked = ["vendor_id", "total_amount", "trip_date"]
    expected_columns_not_present_in_df = ["trip_date"]

    # when
    with pytest.raises(IOError) as execinfo:
       transform.check_if_column_exists_in_df(some_df, column_list_to_be_checked)

    # then
    assert str(execinfo.value) == 'Columns not present in dataframe::- ' + str(expected_columns_not_present_in_df)


@pytest.mark.rename_column_in_df
# when rename_column_in_df is invoked it should rename a column in dataframe if column exists
def test_rename_column_in_df_should_rename_column_if_column_exists(init):
    # given
    test_spark_session = init [1]
    some_df = init[2]
    expected_df = test_spark_session.createDataFrame([(1234,56.76), (2341,76.45)], schema='vendor_id string, net_amount string')
    
    # when
    actual_df = transform.rename_column_in_df(some_df, "total_amount", "net_amount")

    # then
    assert actual_df.collect() == expected_df.collect()
    assert actual_df.schema == expected_df.schema

@pytest.mark.rename_column_in_df
# when rename_column_in_df is invoked it should raise error if column does not exist
def test_rename_column_in_df_should_raise_error_if_column_not_exist(init):
    # ASSEMBLE
    some_df = init [2]

    # ACT
    with pytest.raises(IOError) as execinfo:
       transform.rename_column_in_df(some_df, "trip_date", "trip_date_time")

    # ASSERT
    assert str(execinfo.value) == """Columns not present in dataframe::- ['trip_date']"""

@pytest.mark.typecastcolumns
# when typecastColumns function is invoked should typecast the columns as per the columns_with_data_type_details list
def test_typecastcolumns_should_typecast_dataframe_columns(init):
    # ASSEMBLE
    test_spark_session = init [1]
    some_column_details = init [3]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", StringType(), True),
        StructField("fare", StringType(), True),
        StructField("maximumtemperature", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("weather_date", StringType(), True)])

    expected_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", IntegerType(), True),
        StructField("fare", DoubleType(), True),
        StructField("maximumtemperature", DecimalType(14,4), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("weather_date", DateType(), True)])    

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
        Row("CMT", "2", "12.76", "1.3456", "2014-01-09 20:45:25", "1-1-2014")]), input_schema)    
   
    # ACT
    actual_df = transform.typecastcolumns(input_df, some_column_details)

    # ASSERT
    assert actual_df.schema == expected_schema

@pytest.mark.typecastcolumns
# when typecastColumns function is invoked should throw exception if there is unsupported data type for typecasting
def test_typecastcolumns_should_throw_exception_if_unsupported_datatype_provided(init):
    # ASSEMBLE
    test_spark_session = init [1]
    some_column_details = [["someid", "randomdatatype", ""]]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", StringType(), True),
        StructField("fare", StringType(), True),
        StructField("maximumtemperature", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("weather_date", StringType(), True)])

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
        Row("CMT", "2", "12.76", "1.3456", "2014-01-09 20:45:25", "1-1-2014")]), input_schema)    
   
   # ACT
    with pytest.raises(Exception) as execinfo:
       transform.typecastcolumns(input_df, some_column_details)

    # ASSERT
    assert str(execinfo.value) == """Unsupported data type for typecasting randomdatatype"""

@pytest.mark.filter_records_having_negative_value
#when filterrecordshavingnegativevalue function is invoked
def test_filterrecordshavingnegativevalue_should_filter_out_records_having_negative_value(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", IntegerType(), True),
        StructField("fare", DoubleType(), True)])

    expected_error_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", IntegerType(), True),
        StructField("fare", DoubleType(), True),
        StructField("rejectreason", StringType(), False)])    

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", 2, 2.5), 
       Row("CMT", -1, -2.5), 
       Row("CMT", 4, -2.5), 
       Row("CMT", -1, 5.5), 
       Row("CMT", 0, 6.4), 
       Row("CMT", 1, 0.0)]), input_schema)

    column_names_for_negative_value_check = ["total_cust", "fare"]       
   
    # ACT
    (actual_success_df, actual_error_df) = transform.filter_records_having_negative_value(test_spark_session, input_df, column_names_for_negative_value_check)

    # ASSERT
    expected_success_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", 2, 2.5),
       Row("CMT", 0, 6.4), 
       Row("CMT", 1, 0.0)]), input_schema)

    expected_error_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", -1, -2.5, "total_cust is negative"),
       Row("CMT", -1, -2.5, "fare is negative"),
       Row("CMT", -1, 5.5, "total_cust is negative"),
       Row("CMT", 4, -2.5, "fare is negative")]), expected_error_schema).orderBy(['vendor_id', 'total_cust', 'fare', 'rejectreason'], ascending=True)

    pd.testing.assert_frame_equal(left=expected_success_df.toPandas(),right=actual_success_df.toPandas(),check_exact=True )
    pd.testing.assert_frame_equal(left=expected_error_df.toPandas(),right=actual_error_df.orderBy(['vendor_id', 'total_cust', 'fare', 'rejectreason'], ascending=True).toPandas(), check_exact=True )

@pytest.mark.filter_records_having_negative_value
# when filterrecordshavingnegativevalue function is invoked should throw exception if column is not present
def test_filterrecordshavingnegativevalue_should_throw_exception_if_column_is_not_present(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("total_cust", StringType(), True),
        StructField("fare", StringType(), True),
        StructField("maximumtemperature", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("weather_date", StringType(), True)])

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", 2, 2.5), 
       Row("CMT", -1, -2.5), 
       Row("CMT", 4, -2.5), 
       Row("CMT", -1, 5.5), 
       Row("CMT", 0, 6.4), 
       Row("CMT", 1, 0.0)]), input_schema) 

    column_names_for_negative_value_check = ["total_cust", "fare", "trip_distance", "start_time"]         
   
   # ACT
    with pytest.raises(Exception) as execinfo:
       transform.filter_records_having_negative_value(test_spark_session, input_df, column_names_for_negative_value_check)

    # ASSERT
    assert ( str(execinfo.value) == """Columns not present in dataframe::- ['trip_distance', 'start_time']"""  or  str(execinfo.value) == """Columns not present in dataframe::- ['start_time', 'trip_distance']""")


@pytest.mark.filter_records_having_improper_datetime_value
#when filter_records_having_improper_datetime_value function is invoked it should filter out the records 
#having improper datetime value and return back success & error dataframe
def test_filter_records_having_improper_datetime_value_should_filterout_having_improper_datetime_value(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True)])

    expected_error_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("rejectreason", StringType(), False)])    

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31"), 
       Row("CMT", "-014-01-09 20:45:25", "2014-01-09 20:52:31"), 
       Row("CMT", "2014-01-09 20:45:25", "2333d4-01-09 20:52:31"), 
       Row("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31")]), input_schema)

    column_names_for_improper_datetime_check = ["pickup_datetime", "dropoff_datetime"]

    expected_success_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31")]), input_schema)

    expected_error_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "-014-01-09 20:45:25", "2014-01-09 20:52:31", "pickup_datetime datetime format is incorrect"),
       Row("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31", "pickup_datetime datetime format is incorrect"),
       Row("CMT", "2014-01-09 20:45:25", "2333d4-01-09 20:52:31", "dropoff_datetime datetime format is incorrect"),
       Row("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31", "dropoff_datetime datetime format is incorrect")]), expected_error_schema).orderBy(['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'rejectreason'], ascending=True)
   
   
    # ACT
    (actual_success_df, actual_error_df) = transform.filter_records_having_improper_datetime_value(test_spark_session, input_df, column_names_for_improper_datetime_check)

    # ASSERT
    pd.testing.assert_frame_equal(left=expected_success_df.toPandas(),right=actual_success_df.toPandas(),check_exact=True )
    pd.testing.assert_frame_equal(left=expected_error_df.toPandas(),right=actual_error_df.orderBy(['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'rejectreason'], ascending=True).toPandas(), check_exact=True )

@pytest.mark.filter_records_having_improper_datetime_value
# when filterrecordshavingnegativevalue function is invoked should throw exception if column is not present
def test_filter_records_having_improper_datetime_value_should_throw_exception_if_column_is_not_present(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True)])

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31"), 
       Row("CMT", "-014-01-09 20:45:25", "2014-01-09 20:52:31"), 
       Row("CMT", "2014-01-09 20:45:25", "2333d4-01-09 20:52:31"), 
       Row("CMT", "cdef-01-09 20:45:25", "2333d4-01-09 20:52:31")]), input_schema)

    column_names_for_improper_datetime_check = ["start_date", "pickup_datetime", "end_date", "dropoff_datetime"]

   # ACT
    with pytest.raises(Exception) as execinfo:
       transform.filter_records_having_improper_datetime_value(test_spark_session, input_df, column_names_for_improper_datetime_check)

    # ASSERT
    assert ( str(execinfo.value) == """Columns not present in dataframe::- ['end_date', 'start_date']"""  or  str(execinfo.value) == """Columns not present in dataframe::- ['start_date', 'end_date']""")

@pytest.mark.df_columns_compare
#when df_columns_compare function is invoked 
#it should return success dataframe which matches the compare expression & error dataframe where the compare expression fails
def test_df_columns_compare_should_filter_out_records_which_doesnot_match_compare_expression(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("trip_distance", StringType(), True)])

    expected_error_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("trip_distance", StringType(), True),
        StructField("rejectreason", StringType(), False)])     

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200")]), input_schema) \
       .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType())) \
       .withColumn("dropoff_datetime",col("dropoff_datetime").cast(TimestampType())) \
       .withColumn("trip_distance",col("trip_distance").cast(DoubleType()))

    expected_success_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20")]), input_schema) \
       .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType())) \
       .withColumn("dropoff_datetime",col("dropoff_datetime").cast(TimestampType())) \
       .withColumn("trip_distance",col("trip_distance").cast(DoubleType())) 

    expected_error_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20.0", "pickup_datetime is not < dropoff_datetime"),
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200.0", "pickup_datetime is not < dropoff_datetime"),
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120.0", "trip_distance is not <= 100"),
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200.0", "trip_distance is not <= 100")]), expected_error_schema).orderBy(['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'trip_distance', 'rejectreason'], ascending=True)        

    compare_expressions = ["""pickup_datetime < dropoff_datetime""", """trip_distance <= 100"""]

    # ACT
    (actual_success_df, actual_error_df) = transform.df_columns_compare(test_spark_session, input_df, compare_expressions)

    # ASSERT
    pd.testing.assert_frame_equal(left=expected_success_df.toPandas(),right=actual_success_df.toPandas(),check_exact=True )
    pd.testing.assert_frame_equal(left=expected_error_df.toPandas(),right=actual_error_df.orderBy(['vendor_id', 'pickup_datetime', 'dropoff_datetime','trip_distance', 'rejectreason'], ascending=True).toPandas(), check_exact=True )

@pytest.mark.df_columns_compare
#when df_columns_compare function is invoked 
#it should throw exception if a string column has comparision operator apart from = and !=
def test_df_columns_compare_should_throw_exception_if_string_column_has_comparision_operator(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("trip_distance", StringType(), True)])

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200")]), input_schema) \
       .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType())) \
       .withColumn("dropoff_datetime",col("dropoff_datetime").cast(TimestampType())) \
       .withColumn("trip_distance",col("trip_distance").cast(DoubleType()))

    compare_expressions = ["""vendor_id < dropoff_datetime""", """trip_distance <= 100"""]

    # ACT
    with pytest.raises(Exception) as execinfo:
       transform.df_columns_compare(test_spark_session, input_df, compare_expressions)

    # ASSERT
    assert str(execinfo.value) == """For string datatype compare operator could only be = and !="""

@pytest.mark.df_columns_compare
#when df_columns_compare function is invoked 
#it "should throw exception if comparision operator is apart from =,!=,>,<,>=,<=
def test_df_columns_compare_should_throw_exception_if_invalid_comparision_operator_is_used(init):
    # ASSEMBLE
    test_spark_session = init [1]
    test_spark_context = test_spark_session.sparkContext
    input_schema = StructType([
        StructField("vendor_id", StringType(), True),
        StructField("pickup_datetime", StringType(), True),
        StructField("dropoff_datetime", StringType(), True),
        StructField("trip_distance", StringType(), True)])

    input_df = test_spark_session.createDataFrame(test_spark_context.parallelize([
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 22:45:25", "2014-01-09 20:52:31", "20"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 20:52:31", "120"), 
       Row("CMT", "2014-01-09 20:45:25", "2014-01-09 11:52:31", "200")]), input_schema) \
       .withColumn("pickup_datetime",col("pickup_datetime").cast(TimestampType())) \
       .withColumn("dropoff_datetime",col("dropoff_datetime").cast(TimestampType())) \
       .withColumn("trip_distance",col("trip_distance").cast(DoubleType()))

    compare_expressions = ["""pickup_datetime < dropoff_datetime""", """trip_distance && 100"""]

    # ACT
    with pytest.raises(Exception) as execinfo:
       transform.df_columns_compare(test_spark_session, input_df, compare_expressions)

    # ASSERT
    assert str(execinfo.value) == """Invalid comparison operator!!!!"""