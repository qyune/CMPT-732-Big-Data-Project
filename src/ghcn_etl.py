from pyspark.sql import SparkSession, functions, types

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


country_schema = types.StructType([
    types.StructField('Country_Code', types.StringType()),
    types.StructField('Country', types.StringType()),
])

if __name__ == '__main__':
    spark = SparkSession.builder.appName('ghcn_etl').getOrCreate()
    # The original folder contains records since 18th century. We only read data since 1985 and skip other records.
    df = spark.read.option("header", "false").csv("s3://noaa-ghcn-pds/csv/{198[5-9],199*,20**}.csv", schema=observation_schema)
    df = df.filter(df['observation'] == 'TAVG')
    df = df.filter(df['qflag'].isNull())
    df = df.withColumn('Year', df['date'].substr(0, 4))
    station_avg_df = df.groupby('station', 'Year').agg({'value': 'avg'}).withColumnRenamed('avg(value)', 'TAVG')
    station_avg_df = station_avg_df.withColumn('Country_Code', df['station'].substr(0, 2))
    country_avg_df = station_avg_df.groupby('Country_Code', 'Year').agg({'TAVG': 'avg'}).withColumnRenamed('avg(TAVG)', 'AverageTemperature')
    sc = spark.sparkContext
    # ghcnd-countries.txt is delimited by spaces. But the country names also have spaces.
    # to avoid names like "United States" being separated, we read it as rdd first and use a lambda function to split.
    country_rdd = sc.textFile("s3://noaa-ghcn-pds/ghcnd-countries.txt")
    country_rdd = country_rdd.map(lambda x: (x[0: 2], x[3:]))
    country_table = spark.createDataFrame(country_rdd, country_schema)
    country_table = country_table.withColumn('Country', functions.trim(country_table['Country']))
    result_df = country_avg_df.join(country_table.hint("broadcast"), on='Country_Code')
    result_df = result_df.withColumn('AverageTemperature', functions.round((result_df['AverageTemperature']) / 10, 4))
    # As an aggregated dataframe, result_df is less than 10,000 rows.
    # It is fine to coalesce it into a single file instead of being many small pieces.
    result_df.coalesce(1).write.option("header", "true").csv("s3://climate-data-732/AverageTemperatureByCountryYear", mode="overwrite")
