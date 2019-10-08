import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType

# The date format string preferred to our work here: YYYY-MM-DD
date_format = "%Y-%m-%d"

# The AWS key id and password are configured in a configuration file "dl.cfg"
config = configparser.ConfigParser()
config.read('dl.cfg')

# Reads and saves the AWS access key information and saves them in a environment variable
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
OUTPUT = config['ETL']['OUTPUT_DATA']

def create_spark_session():
    """
    This function creates a session with Spark, the entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = SparkSession.builder.config("spark.jars.packages",
                                        "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
    .enableHiveSupport().getOrCreate()
    return spark

def read_data(spark, input_path, input_format = "csv", columns = '*', debug_size = None, **options):
    """
    Loads data from a data source using the pyspark module and returns it as a spark 'DataFrame'.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        input_format (:obj:`str`): Optional string for format of the data source. Default to 'csv'.
        columns (:obj:`list`): List of columns of the dataframe to return. Default to "*", which means 'all columns'.
        debug_size (int): Define the number of rows to read for debug purposes. The default value None means 'all rows'.
        options: All other string options.
    """
    if debug_size is None:
        df = spark.read.load(input_path, format=input_format, **options).select(columns)
    else:
        df = spark.read.load(input_path, format=input_format, **options).select(columns).limit(debug_size)
    return df

def save(df, output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None, **options):
    """
    Saves the contents of the DataFrame to a data source.

    The data source is specified by the format and a set of options. If format is not specified, 'parquet' will be used.
    
    Args:
        df (:obj:`DataFrame`): Spark DataFrame.
        output_path (:obj:`str`): The path in a Hadoop supported file system where the DataFrame contentes will be saved.
        mode (:obj:`str`): Specifies the behavior of the save operation when data already exists. Default to 'overwrite'.
        output_format (:obj:`str`): Optional string for format of the data source to be saved. Default to 'parquet'.
        columns (:obj:`list`): List of columns of the dataframe to save. Default to "*", which means 'all columns'.
        partitionBy (:obj:`list`): Names of partitioning columns. The default value None means 'no partitions'.
        options: All other string options.
    """

    df.select(columns).write.save(output_path, mode= mode, format=output_format, partitionBy = partitionBy, **options)
    
def etl_immigration_data(spark, input_path="immigration_data_sample.csv", output_path="out/immigration.parquet",
                         date_output_path="out/date.parquet",
                         input_format = "csv", columns = ['i94addr', 'i94mon','cicid','i94visa','i94res','arrdate','i94yr','depdate',
                                                          'airline', 'fltno', 'i94mode', 'i94port', 'visatype', 'gender', 
                                                          'i94cit', 'i94bir'], 
                         load_size = None, partitionBy = ["i94yr", "i94mon"], columns_to_save='*', header=True, **options):
    """
    Reads the immigration dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter 
    out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
        date_output_path (:obj:`str`): Directory where to save date output files.
        input_format (:obj:`str`): Type of the input files. Default to "csv" (comma-separated value).
        columns (:obj:`list`): List of the columns names to read in. Useful when only some columns are useful.
        load_size (int): Number of rows to read for debug purposes.
        partitionBy (:obj:`list`): Files will be saved in partitions using the columns of this list.
        columns_to_save (:obj:`list`): Define what columns will be saved.
        header: (bool): Uses the first line as names of columns. If None is set, it uses the default value, false.
        options: All other string options.
    """
    
    # Loads the immigration dataframe using Spark
    # We discard the columns ['admnum', 'biryear', 'count', 'dtaddto', 'dtadfile', 'entdepa', 'entdepd', 'entdepu', 'insnum', 'matflag', 'occup', 'visapost'] as they seemed not to be very useful for our goals.
    # Some of them were very unclear of what they really represent.
    immigration = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)
    
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 
        'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count', 'biryear', 'dtadfile', 'depdate']
    
    date_cols = ['arrdate', 'depdate']
    
    high_null = ["visapost", "occup", "entdepu", "insnum"]
    not_useful_cols = ["count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    
    # Convert columns read as string/double to integer
    immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    # Convert SAS date to a meaningful string date in the format of YYYY-MM-DD
    immigration = convert_sas_date(immigration, date_cols)
    
    # Drop high null columns and not useful columns
    immigration = immigration.drop(*high_null)
    immigration = immigration.drop(*not_useful_cols)
    
    # Create a new columns to store the length of the visitor stay in the US
    immigration = immigration.withColumn('stay', date_diff_udf(immigration.arrdate, immigration.depdate))
    immigration = cast_type(immigration, {'stay': IntegerType()})
    
    # Generate DATE dataframe and save it to the output_path indicated as parameter of the function
    if date_output_path is not None:
        arrdate = immigration.select('arrdate').distinct()
        depdate = immigration.select('depdate').distinct()
        dates = arrdate.union(depdate)
        dates = dates.withColumn("date", to_date(dates.arrdate, date_format))
        dates = dates.withColumn("year", year(dates.date))
        dates = dates.withColumn("month", month(dates.date))
        dates = dates.withColumn("day", dayofmonth(dates.date))
        dates = dates.withColumn("weekofyear", weekofyear(dates.date))
        dates = dates.withColumn("dayofweek", dayofweek(dates.date))
        dates = dates.drop("date").withColumnRenamed('arrdate', 'date')
        save(df=dates.select("date", "year", "month", "day", "weekofyear", "dayofweek"), output_path=date_output_path)
    
    # Save the processed immigration dataset to the output_path
    if output_path is not None:
        save(df=immigration.select(columns_to_save), output_path=output_path, partitionBy = partitionBy)
    return immigration

def etl_temperature_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", output_path="out/temperature.parquet", 
                         input_format = "csv", columns = '*', load_size = None, partitionBy = ["Country", "City"], header=True, **options):
    """
    Reads the global temperature dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
        input_format (:obj:`str`): Type of the input files. Default to "csv" (comma-separated value).
        columns (:obj:`list`): List of the columns names to read in. Useful when only some columns are useful.
        load_size (int): Number of rows to read for debug purposes.
        partitionBy (:obj:`list`): Files will be saved in partitions using the columns of this list.
        header: (bool): Uses the first line as names of columns. If None is set, it uses the default value, false.
        options: All other string options.
    """
    # Loads the global temperature dataframe using Spark
    temperature = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)
    # Save the temperature dataset to the output_path
    save(df=temperature, output_path=output_path, partitionBy = partitionBy)
    return temperature

def etl_airport_data(spark, input_path="airport-codes_csv.csv", output_path="out/airport.parquet", 
                         input_format = "csv", columns = '*', load_size = None, partitionBy = ["iso_country"], header=True, **options):
    """
    Reads the airport dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
        input_format (:obj:`str`): Type of the input files. Default to "csv" (comma-separated value).
        columns (:obj:`list`): List of the columns names to read in. Useful when only some columns are useful.
        load_size (int): Number of rows to read for debug purposes.
        partitionBy (:obj:`list`): Files will be saved in partitions using the columns of this list.
        header: (bool): Uses the first line as names of columns. If None is set, it uses the default value, false.
        options: All other string options.
    """
    # Loads the airport dataframe using Spark
    airport = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)    
    # Save the airport dataset to the output_path
    save(df=airport, output_path=output_path, partitionBy = partitionBy)
    return airport

def etl_demographics_data(spark, input_path="us-cities-demographics.csv", output_path="out/demographics.parquet", 
                         input_format = "csv", columns='*',
                          load_size = None, partitionBy = ["State Code"], header=True, sep=";", **options):
    """
    Reads the demographics dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
        input_format (:obj:`str`): Type of the input files. Default to "csv" (comma-separated value).
        columns (:obj:`list`): List of the columns names to read in. Useful when only some columns are useful.
        load_size (int): Number of rows to read for debug purposes.
        partitionBy (:obj:`list`): Files will be saved in partitions using the columns of this list.
        header: (bool): Uses the first line as names of columns. If None is set, it uses the default value, false.
        options: All other string options.
    """
    # Loads the demographics dataframe using Spark
    demographics = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, sep=sep, **options)
    
    # Convert numeric columns to the proper types: Integer and Double
    int_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_cols = ['Median Age', 'Average Household Size']
    demographics = cast_type(demographics, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))
    
    first_agg = {"Median Age": "first", "Male Population": "first", "Female Population": "first", 
                 "Total Population": "first", "Number of Veterans": "first", "Foreign-born": "first", "Average Household Size": "first"}
    # First aggregation - City
    agg_df = demographics.groupby(["City", "State", "State Code"]).agg(first_agg)
    # Pivot Table to transform values of the column Race to different columns
    piv_df = demographics.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    # Rename column names removing the spaces to avoid problems when saving to disk (we got errors when trying to save column names with spaces)
    demographics = agg_df.join(other=piv_df, on=["City", "State", "State Code"], how="inner")\
    .withColumnRenamed('first(Total Population)', 'TotalPopulation')\
    .withColumnRenamed('first(Female Population)', 'FemalePopulation')\
    .withColumnRenamed('first(Male Population)', 'MalePopulation')\
    .withColumnRenamed('first(Median Age)', 'MedianAge')\
    .withColumnRenamed('first(Number of Veterans)', 'NumberVeterans')\
    .withColumnRenamed('first(Foreign-born)', 'ForeignBorn')\
    .withColumnRenamed('first(Average Household Size)', 'AverageHouseholdSize')\
    .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')\
    .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican')\
    .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')
    
    numeric_cols = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation', 'AverageHouseholdSize',
                    'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']
    # Fill the null values with 0
    demographics = demographics.fillna(0, numeric_cols)
    
    # Save the demographics dataset to the output_path
    if output_path is not None:
        save(df=demographics, output_path=output_path, partitionBy = partitionBy)
    
    return demographics

def etl_states_data(spark, output_path="out/state.parquet"):
    cols = ['TotalPopulation', 'FemalePopulation', 'MalePopulation', 'NumberVeterans', 'ForeignBorn', 
            'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']
    """
    Reads the states dataset indicated in the input_path, performs the ETL process and saves it in the output path indicated by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        output_path (:obj:`str`): Directory where to save immigration output files.
    """
    # Loads the demographics dataframe using Spark
    demographics = etl_demographics_data(spark, output_path=None)
    # Aggregates the dataset by State
    states = demographics.groupby(["State Code", "State"]).agg(dict(zip(cols, len(cols)*["sum"])))
    # Loads the lookup table I94ADDR
    addr = read_data(spark, input_path="lookup/I94ADDR.csv", input_format="csv", columns="*", header=True)\
    .withColumnRenamed('State', 'State Original')
    
    # Join the two datasets
    addr = addr.join(states, states["State Code"] == addr.Code, "left")
    addr = addr.withColumn("State", when(isnull(addr["State"]), capitalize_udf(addr['State Original'])).otherwise(addr["State"]))
    addr = addr.drop('State Original', 'State Code')
    
    cols = ['sum(BlackOrAfricanAmerican)', 'sum(White)', 'sum(AmericanIndianAndAlaskaNative)',
            'sum(HispanicOrLatino)', 'sum(Asian)', 'sum(NumberVeterans)', 'sum(ForeignBorn)', 'sum(FemalePopulation)', 
            'sum(MalePopulation)', 'sum(TotalPopulation)']
    
    # Rename the columns to modify default names returned when Spark aggregates the values of the columns.
    # For example: column 'sum(MalePopulation)' becomes 'MalePopulation'
    mapping = dict(zip(cols, [re.search(r'\((.*?)\)', c).group(1) for c in cols]))
    addr = rename_columns(addr, mapping)
    
    # Save the resulting dataset to the output_path
    if output_path is not None:
        save(df=addr, output_path=output_path)
    return addr
    
def etl_countries_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", output_path="out/country.parquet", 
                         input_format = "csv", columns = '*', load_size = None, header=True, **options):
    """
    Reads the global temperatures dataset indicated in the input_path and transform it to generate the country dataframe. Performs the ETL process and saves it in the output path indicated by the parameter out_put path.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_path (:obj:`str`): Directory where to find the input files.
        output_path (:obj:`str`): Directory where to save immigration output files.
        input_format (:obj:`str`): Type of the input files. Default to "csv" (comma-separated value).
        columns (:obj:`list`): List of the columns names to read in. Useful when only some columns are useful.
        load_size (int): Number of rows to read for debug purposes.
        header: (bool): Uses the first line as names of columns. If None is set, it uses the default value, false.
        options: All other string options.
    """
    # Loads the demographics dataframe using Spark
    countries = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)
    # Aggregates the dataset by Country and rename the name of new columns
    countries = countries.groupby(["Country"]).agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    # Rename specific country names to match the I94CIT_I94RES lookup table when joining them
    change_countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), ("Country", "Côte D'Ivoire", "Ivory Coast")]
    countries = change_field_value_condition(countries, change_countries)
    countries = countries.withColumn('Country_Lower', lower(countries.Country))
    
    # Rename specific country names to match the demographics dataset when joining them
    change_res = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"), 
                  ("I94CTRY", "INVALID: CANADA", "CANADA"),
                  ("I94CTRY", "CHINA, PRC", "CHINA"),
                  ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                  ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                  ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]
    
    # Loads the lookup table I94CIT_I94RES
    res = read_data(spark, input_path="lookup/I94CIT_I94RES.csv", input_format=input_format, columns="*",
                          debug_size = load_size, header=header, **options)
    res = cast_type(res, {"Code": IntegerType()})
    res = change_field_value_condition(res, change_res)
    res = res.withColumn('Country_Lower', lower(res.I94CTRY))
    # Join the two datasets to create the country dimmension table
    res = res.join(countries, res.Country_Lower == countries.Country_Lower, how="left")
    res = res.withColumn("Country", when(isnull(res["Country"]), capitalize_udf(res.I94CTRY)).otherwise(res["Country"]))   
    res = res.drop("I94CTRY", "Country_Lower")
    
    # Save the resulting dataset to the output_path
    if output_path is not None:
        save(df=res, output_path=output_path)
    return res

def cast_type(df, cols):
    """
    Convert the types of the columns according to the configuration supplied in the cols dictionary in the format {"column_name": type}
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        cols (:obj:`dict`): Dictionary in the format of {"column_name": type} indicating what columns and types they should be converted to
    """
    for k,v in cols.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df

def convert_sas_date(df, cols):
    """
    Convert dates in the SAS datatype to a date in a string format YYYY-MM-DD
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        cols (:obj:`list`): List of columns in the SAS date format to be convert
    """
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

def change_field_value_condition(df, change_list):
    '''
    Helper function used to rename column values based on condition.
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        change_list (:obj: `list`): List of tuples in the format (field, old value, new value)
    '''
    for field, old, new in change_list:
        df = df.withColumn(field, when(df[field] == old, new).otherwise(df[field]))
    return df

def rename_columns(df, mapping):
    '''
    Rename the columns of the dataset based in the mapping dictionary
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        mapping (:obj: `dict`): Mapping dictionary in the format {old_name: new_name}
    '''
    df = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    return df

def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days

# User defined functions using Spark udf wrapper function to convert SAS dates into string dates in the format YYYY-MM-DD, to capitalize the first letters of the string and to calculate the difference between two dates in days.
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
capitalize_udf = udf(lambda x: x if x is None else x.title())
date_diff_udf = udf(date_diff)

if __name__ == "__main__" :
    spark = create_spark_session()
    # Perform ETL process for the Immigration dataset generating immigration and date tables and save them in the S3 bucket indicated in the output_path parameters.
    immigration = etl_immigration_data(spark, input_path='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                                     output_path="s3a://data-engineer-capstone/immigration.parquet",
                                     date_output_path="s3a://data-engineer-capstone/date.parquet",
                                     input_format = "com.github.saurfang.sas.spark", 
                                     load_size=1000, partitionBy=None, 
                                     columns_to_save = '*')
    # Perform ETL process for the Country table. Generating the Country table and saving it in the S3 bucket indicated in the output_path parameter.
    countries = e.etl_countries_data(spark, output_path=e.OUTPUT + "country.parquet")
    # Perform ETL process for the State table. Generating the State table and saving it in the S3 bucket indicated in the output_path parameter.
    states = e.etl_states_data(spark, output_path=e.OUTPUT + "state.parquet")