import pandas as pd
import numpy as np
import boto3
import json
import sys
import re
from pyspark.sql import SparkSession, functions, types, Row
from sqlalchemy import create_engine
import country_converter as coco

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

observation_schema = types.StructType([
    types.StructField('dt', types.TimestampType(), False),
    types.StructField('AverageTemperature', types.FloatType(), False),
    types.StructField('AverageTemperatureUncertainty', types.FloatType(), False),
    types.StructField('City', types.StringType(), False),
    types.StructField('Country', types.StringType(), False),
    types.StructField('Latitude', types.StringType(), False),
    types.StructField('Longitude', types.StringType(), False)
])


@functions.udf(returnType=types.StringType())
def country_convert(country):
    """Converting a country name to country code (ISO-3)"""
    return coco.convert(names=country, to='iso3')

# Connecting to AWS RDS (Postgresql Engine) - output
engine = create_engine(
    'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')


def main():
    # Reads input source from AWS S3 bucket
    temperature_df = spark.read.format('csv').options(header='true', schema='observation_schema') \
        .option("mode", "DROPMALFORMED").load("s3://climate-data-732/GlobalLandTemperaturesByCity.csv")
    # .option("mode", "DROPMALFORMED").load("GlobalLandTemperaturesByCity.csv")

    temperature_df.createOrReplaceTempView("temperature_view")
    temperature_df = spark.sql("SELECT dt, AverageTemperature, City, Country "
                               "FROM temperature_view "
                               "WHERE dt >= '1985-01-01'")

    temperature_df.dropna(subset=('AverageTemperature', 'Country'))
    temperature_df.cache()

    year_df = temperature_df.select(functions.date_format('dt', 'yyy').alias('Year'),
                                    'AverageTemperature',
                                    'City',
                                    'Country')

    # Type conversion to apply aggregate function
    year_df = year_df.withColumn('AverageTemperature', year_df['AverageTemperature'].cast(types.FloatType()))

    avg_temp_df = year_df.groupBy('Year', 'Country').agg(functions.avg('AverageTemperature').alias('Avg_Temp'))

    avg_temp_df_world = year_df.groupBy('Year').agg(
        functions.avg('AverageTemperature').alias('Avg_Temp_World')).orderBy('Year')
    avg_temp_df_world = avg_temp_df_world.withColumn('Avg_Temp_World',
                                                     functions.round(avg_temp_df_world['Avg_Temp_World'], 2))

    # Save to DB P2-2
    avg_temp_df_world.toPandas().to_sql('graph_2_2', engine, if_exists='replace')

    avg_temp_df.select(functions.countDistinct("Country"))

    avg_temp_df.createOrReplaceTempView("avg_temp_view")

    avg_temp_df_85 = spark.sql("SELECT * "
                               "FROM  avg_temp_view "
                               "WHERE YEAR = 1985")
    avg_temp_df_85.createOrReplaceTempView("avg_temp_85_view")

    avg_temp_df_12 = spark.sql("SELECT * "
                               "FROM  avg_temp_view "
                               "WHERE YEAR = 2012")
    avg_temp_df_12.createOrReplaceTempView("avg_temp_12_view")

    joined_df = spark.sql("SELECT y1.Country, "
                          "y1.Year AS Year_85, y2.Year AS Year_12, "
                          "y1.Avg_Temp AS Avg_85, y2.Avg_Temp AS Avg_12 "
                          "FROM avg_temp_85_view y1 "
                          "JOIN avg_temp_12_view y2 "
                          "ON y1.Country = y2.Country")

    diff_df = joined_df.withColumn('Diff', (joined_df['Avg_12'] - joined_df['Avg_85'])).orderBy('Country')

    # Adding country code column
    code_df = diff_df.withColumn('Code', country_convert(diff_df['Country']))

    # adjust floating point precision to 2
    res_df = code_df.withColumn('Avg_85', functions.round(code_df['Avg_85'], 2))
    res_df = res_df.withColumn('Avg_12', functions.round(code_df['Avg_12'], 2))
    res_df = res_df.withColumn('Diff', functions.round(code_df['Diff'], 2))

    # Save to DB P2-1
    res_df.toPandas().to_sql('graph_2_1', engine, if_exists='replace')

    rank_df = res_df.orderBy('Diff', ascending=False)

    # Save to DB P2-3
    rank_df.toPandas().to_sql('graph_2_3', engine, if_exists='replace')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('correlate logs').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
