from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import when, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window 
import pandas as pd
import numpy as np
import sys
import os
import random, csv, shutil
import traceback
from ast import literal_eval

# Currently hardcoded. Will be parameterized to pass into the script as arguments
input_file = "MOCK_DATA_NEW.csv"
output_directory = "output"
output_file_name = "parquetWithMetaNov1"
res_dict = {}
meta = {"PII": "True"}

# Create spark session
spark = SparkSession.builder.appName('MetadataTagging').getOrCreate()

# Read the data file and create a Spark Dataframe
try:
    if input_file.endswith('.txt'):
        df = spark.read.option("header", "true") \
        .option("delimiter", "|") \
        .option("inferSchema", "true") \
        .csv(input_file)
    elif input_file.endswith('.csv'):
        df = spark.read.csv(input_file,inferSchema =True,header=True)
    else:
#         f.write(dataFileFormatInvalid)
#         f.close()
        sys.exit("input file format is invalid")
except Exception as e:
#     f.write(dataFileInvalid)
#     f.close()
    sys.exit("exception while reading input file")

# Extract the columns to be able to loop through them and check for regex matches
df_columns = df.columns
for column in df_columns:
    regex_search = "\d{3}-\d{2}-\d{4}|[345]\d{15}|\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}|[0-9a-f]{2}([-:]?)[0-9a-f]{2}(\\1[0-9a-f]{2}){4}$"
    column_name=column+'_'
    df = df.withColumn(column_name,col(column).rlike(regex_search))

# Loop through to get the occurence of boolean values as a result of "rlike" in line #51. Distinct function, by default returns values in a descending order as True, False
for column in df_columns:
    check_bool = df.select(column+'_').distinct().collect()[0][0]
    res_dict[column] = check_bool
    df = df.drop(column+'_')

# Tag sensitive element's metadata with the value {"PII":"True"}
for key, value in res_dict.items():
    if value == True:
        df = df.withColumn(key, col(key).alias(key, metadata=meta))

# Write out the file containing the updated metadata
df.coalesce(1).write.format('parquet').save(output_directory+'/'+output_file_name)