from pyspark.sql import SparkSession, functions, types
from sqlalchemy import create_engine

observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
])


if __name__ == '__main__':
    spark = SparkSession.builder.appName('page three').getOrCreate()
    df = spark.read.option("header", "false").csv("s3://noaa-ghcn-pds/csv/{198[5-9],199*,20**}.csv",
                                                  schema=observation_schema)
    df = df.filter(df['observation'] == 'TAVG')
    df = df.filter(df['qflag'].isNull())
    df = df.withColumn('Year', df['date'].substr(0, 4))
    df.write.option("header", "true").csv("s3://climate-data-732/ghch_temp_1985_2020.csv", mode="overwrite")
    annual_temp_df = df.groupby('Year').agg({'value': 'avg'}).withColumnRenamed('avg(value)', 'AverageTemperature')
    annual_temp_df = annual_temp_df.withColumn('AverageTemperature',
                                               functions.round((annual_temp_df['AverageTemperature']) / 10, 1))
    flood_df = spark.read.option("header", "true").csv("s3://climate-data-732/FloodArchiveRaw.csv")
    flood_df = flood_df.withColumn('Year', flood_df['Began'].substr(0, 4))
    annual_flood_df = flood_df.groupby('Year').count().withColumnRenamed('count', 'FloodOccurrences')
    joined_df = annual_temp_df.join(annual_flood_df.hint('broadcast'), on='Year')
    joined_df.cache()
    corr = joined_df.corr('AverageTemperature', 'FloodOccurrences')
    print('Correlation coefficient between AverageTemperature and FloodOccurrences is %s.' % corr)
    pdf = joined_df.toPandas()
    engine = create_engine(
        'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
    pdf.to_sql('graph_3_1', engine, if_exists='replace')



