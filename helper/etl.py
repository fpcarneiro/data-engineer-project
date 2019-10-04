import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, to_date

date_format = "%Y-%m-%d"

config = configparser.ConfigParser()
config.read('dl.cfg')

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

    The data source is specified by the format and a set of options. If format is not specified, 'csv' will be used.
    
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
                         input_format = "csv", columns = ['i94addr', 'i94mon','cicid','i94visa','i94res','arrdate','i94yr','depdate',
                                                          'airline', 'fltno', 'i94mode', 'i94port', 'visatype', 'gender', 
                                                          'i94cit', 'i94bir'], 
                         load_size = None, partitionBy = ["i94yr", "i94mon"], header=True, **options):
    """
    This function reads the songs JSON files from S3 and processes them with Spark. We separate the files into specific dataframes the represent the tables in our star schema model.
    Then, these tables are saved back to the output folder indicated by output_data parameter.
    
    Args:
        spark (:obj:`SparkSession`): Spark session. 
            Represents the entry point to programming Spark with the Dataset and DataFrame API.
        input_data (:obj:`str`): Directory where to find the input files.
        output_data (:obj:`str`): Directory where to save parquet files.
    """   
    immigration = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)
    
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 
        'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count', 'biryear', 'dtadfile', 'depdate']
    
    date_cols = ['arrdate', 'depdate']
    
    high_null = ["visapost", "occup", "entdepu", "insnum"]
    not_useful_cols = ["count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    
    immigration = cast_type(immigration, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    
    immigration = convert_sas_date(immigration, date_cols)
    
    immigration = immigration.drop(*high_null)
    immigration = immigration.drop(*not_useful_cols)
    
    immigration = immigration.withColumn('stay', date_diff_udf(immigration.arrdate, immigration.depdate))
    
    save(df=immigration, output_path=output_path, partitionBy = partitionBy)
    return immigration

def etl_temperature_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", output_path="out/temperature.parquet", 
                         input_format = "csv", columns = '*', load_size = None, partitionBy = ["Country", "City"], header=True, **options): 
    temperature = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)    
    save(df=temperature, output_path=output_path, partitionBy = partitionBy)
    return temperature

def etl_airport_data(spark, input_path="airport-codes_csv.csv", output_path="out/airport.parquet", 
                         input_format = "csv", columns = '*', load_size = None, partitionBy = ["iso_country"], header=True, **options): 
    airport = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)    
    save(df=airport, output_path=output_path, partitionBy = partitionBy)
    return airport

def etl_demographics_data(spark, input_path="us-cities-demographics.csv", output_path="out/demographics.parquet", 
                         input_format = "csv", columns='*',
                          load_size = None, partitionBy = ["State Code"], header=True, sep=";", **options):
    
    demographics = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, sep=sep, **options)
    
    int_cols = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_cols = ['Median Age', 'Average Household Size']
    demographics = cast_type(demographics, dict(zip(int_cols, len(int_cols)*[IntegerType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))
    
    first_agg = {"Median Age": "first", "Male Population": "first", "Female Population": "first", 
                 "Total Population": "first", "Number of Veterans": "first", "Foreign-born": "first", "Average Household Size": "first"}
    
    agg_df = demographics.groupby(["City", "State", "State Code"]).agg(first_agg)
    piv_df = demographics.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    demographics = agg_df.join(other=piv_df, on=["City", "State", "State Code"], how="inner")\
    .withColumnRenamed('first(Total Population)', 'TotalPopulation')\
    .withColumnRenamed('first(Female Population)', 'FemalePopulation')\
    .withColumnRenamed('first(Male Population)', 'MalePopulation')\
    .withColumnRenamed('first(Median Age)', 'MedianAge')\
    .withColumnRenamed('first(Number of Veterans)', 'NumberVeterans')\
    .withColumnRenamed('first(Foreign-born)', 'ForeignBorn')\
    .withColumnRenamed('first(Average Household Size)', 'AverageHouseholdSize')\
    .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')\
    .withColumnRenamed('Black or African-American', 'BlackOrAfrican-American')\
    .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')
    
    numeric_cols = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation', 'AverageHouseholdSize',
                    'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfrican-American', 'HispanicOrLatino', 'White']
    demographics = demographics.fillna(0, numeric_cols)
    
    if output_path is not None:
        save(df=demographics, output_path=output_path, partitionBy = partitionBy)
        
    return demographics

def etl_states_data(spark, output_path="out/state.parquet"):
    cols = ['TotalPopulation', 'FemalePopulation', 'MalePopulation', 'NumberVeterans', 'ForeignBorn', 
            'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfrican-American', 'HispanicOrLatino', 'White']
    
    demographics = etl_demographics_data(spark, output_path=None)
    states = demographics.groupby(["State Code", "State"]).agg(dict(zip(cols, len(cols)*["sum"])))
    addr = read_data(spark, input_path="lookup/I94ADDR.csv", input_format="csv", columns="*", header=True)\
    .withColumnRenamed('State', 'State Original')
    
    addr = addr.join(states, states["State Code"] == addr.Code, "left")
    addr = addr.withColumn("State", when(isnull(addr["State"]), capitalize_udf(addr['State Original'])).otherwise(addr["State"]))
    addr = addr.drop('State Original', 'State Code')
    
    cols = ['sum(BlackOrAfrican-American)', 'sum(White)', 'sum(AmericanIndianAndAlaskaNative)',
            'sum(HispanicOrLatino)', 'sum(Asian)', 'sum(NumberVeterans)', 'sum(ForeignBorn)', 'sum(FemalePopulation)', 
            'sum(MalePopulation)', 'sum(TotalPopulation)']
    
    mapping = dict(zip(cols, [re.search(r'\((.*?)\)', c).group(1) for c in cols]))
    addr = rename_columns(addr, mapping)
    
    if output_path is not None:
        save(df=addr, output_path=output_path)
    return addr
    
def etl_countries_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", output_path="out/country.parquet", 
                         input_format = "csv", columns = '*', load_size = None, header=True, **options):
    
    countries = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = load_size, header=header, **options)
    
    countries = countries.groupby(["Country"]).agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    change_countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), ("Country", "Côte D'Ivoire", "Ivory Coast")]
    countries = change_field_value_condition(countries, change_countries)
    countries = countries.withColumn('Country_Lower', lower(countries.Country))
    
    change_res = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"), 
                  ("I94CTRY", "INVALID: CANADA", "CANADA"),
                  ("I94CTRY", "CHINA, PRC", "CHINA"),
                  ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                  ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                  ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]
    
    res = read_data(spark, input_path="lookup/I94CIT_I94RES.csv", input_format=input_format, columns="*",
                          debug_size = load_size, header=header, **options)
    res = change_field_value_condition(res, change_res)
    res = res.withColumn('Country_Lower', lower(res.I94CTRY))
    res = res.join(countries, res.Country_Lower == countries.Country_Lower, how="left")
    res = res.withColumn("Country", when(isnull(res["Country"]), capitalize_udf(res.I94CTRY)).otherwise(res["Country"]))   
    res = res.drop("I94CTRY", "Country_Lower")
    
    save(df=res, output_path=output_path)
    return res

def cast_type(df, cols):
    for k,v in cols.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df

def convert_sas_date(df, cols):
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df

def change_field_value_condition(df, change_list):
    for field, old, new in change_list:
        df = df.withColumn(field, when(df[field] == old, new).otherwise(df[field]))
    return df

def rename_columns(df, mapping):
    df = df.select([col(c).alias(mapping.get(c, c)) for c in df.columns])
    return df

def date_diff(date1, date2):
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days
    
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))

capitalize_udf = udf(lambda x: x if x is None else x.title())

date_diff_udf = udf(date_diff)