import pandas as pd
import numpy as np
import boto3
import json
import sys
import re
from pyspark.sql import SparkSession, functions, types, Row
from sqlalchemy import create_engine
import country_converter as coco
import pycountry_convert as pc


@functions.udf(returnType=types.StringType())
def country_convert(name):
    cc = coco.CountryConverter(only_UNmember=True)
    code = cc.convert(names=name, to='iso2')
    return code


@functions.udf(returnType=types.StringType())
def country_convert_to_continent(code):
    """map a country code(ISO-2) to continent (ISO-2)"""
    return pc.country_alpha2_to_continent_code(code)


# Connecting to AWS RDS (Postgresql Engine) - output
engine = create_engine(
    'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')


def main():
    # Reads input source from AWS S3 bucket
    temperature_df = spark.read.format('csv').options(header='true', schema='observation_schema') \
        .option("mode", "DROPMALFORMED").load(
        "s3://climate-data-732/AverageTemperatureByCountryYear.csv/part-00000-6aae2693-38c5-446e-a7bd-07a22581336e-c000.csv")

    temperature_df.cache()

    # Adding country code column
    code_df = temperature_df.withColumn('Code_iso2', country_convert(temperature_df['Country']))
    code_df = code_df.filter(code_df['Code_iso2'] != 'not found')
    code_df = code_df.withColumn('Continent', country_convert_to_continent(code_df['Code_iso2']))

    code_df.show()
    # Save to DB P2-4
    code_df.to_sql('graph_2_4', engine, if_exists='replace')


if __name__ == '__main__':
    spark = SparkSession.builder.appName('Worlds Temperature_GHCN').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
