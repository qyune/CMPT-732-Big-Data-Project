import pandas as pd
import boto3
import json
from pyspark.sql import SparkSession, functions, types
from sqlalchemy import create_engine


@functions.udf(returnType=types.ArrayType(types.StringType()))
def maincause_column_cleaning(text):
    if text is None:
        return ''
    text = text.strip().lower().replace('rains', 'rain').replace('  ', ' ')
    if 'tropical storm' in text:
        text = 'tropical storms'
    elif 'tropical cyclon' in text:
        text = 'tropical cyclons'
    text_list = [i.strip() for i in text.split('and')]
    return text_list


if __name__ == '__main__':
    spark = SparkSession.builder.appName('page one').getOrCreate()
    engine = create_engine(
        'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
    df = spark.read.option("header", "true").csv("s3://climate-data-732/FloodArchiveRaw.csv")
    df = df.withColumn('Decade', functions.concat(df['Began'].substr(0, 3), functions.lit('0s')))
    df = df.withColumn('Country', functions.trim(df['Country']))
    df = df.withColumn('Country', functions.regexp_replace('Country', '\xa0', ''))
    # Get the country name dict from json to correct wrong country names
    s3 = boto3.resource('s3')
    obj = s3.Object('climate-data-732', 'country_dict.json')
    file_content = obj.get()['Body'].read().decode('utf-8')
    country_dict = json.loads(file_content)
    df = df.replace(country_dict, 'Country')
    df.write.option("header", "true").csv("s3://climate-data-732/FloodArchiveCleaned.csv", mode="overwrite")
    df.cache()

    df = df.withColumn('CleanedCause', maincause_column_cleaning(df['MainCause']))
    df_30 = df.filter(df['Decade'].isin(['1990s', '2000s', '2010s']))
    df_30 = df_30.select(functions.explode(df_30['CleanedCause']).alias('SplitedCause'), 'Decade')
    decade_cause_count = df_30.groupby('Decade', 'SplitedCause').count()
    decade_cause_count.toPandas().to_sql('graph_1_2', engine, if_exists='replace')

    country_count = df.groupby('Country').count()
    country_count.toPandas().to_sql('graph_1_3', engine, if_exists='replace')