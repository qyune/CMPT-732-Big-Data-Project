from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from sqlalchemy import create_engine

station_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('lat', types.StringType()),
    types.StructField('long', types.StringType()),
])

if __name__ == '__main__':
    spark = SparkSession.builder.appName('machine learning').getOrCreate()
    round_digit = 0
    df = spark.read.option("header", "true").csv("s3://climate-data-732/ghch_temp_1985_2020.csv")
    station_annual_df = df.groupby('station', 'year').agg({'value': 'avg'}).withColumnRenamed('avg(value)',
                                                                                              'AverageTemperature')
    station_rdd = sc.textFile("s3://noaa-ghcn-pds/ghcnd-stations.txt")
    station_rdd = station_rdd.map(lambda x: (x[0:11], x[12:20], x[21:30]))
    station_df = spark.createDataFrame(station_rdd, station_schema)
    station_df = station_df.withColumn('lat', functions.trim(station_df['lat']).astype('double'))
    station_df = station_df.withColumn('long', functions.trim(station_df['long']).astype('double'))
    station_df = station_df.withColumn('lat', functions.round(station_df['lat'], round_digit))
    station_df = station_df.withColumn('long', functions.round(station_df['long'], round_digit))
    station_annual_df = station_annual_df.join(station_df.hint('broadcast'), on='station')
    location_annual_df = station_annual_df.groupby('year', 'lat', 'long').agg({'AverageTemperature': 'avg'}) \
        .withColumnRenamed('avg(AverageTemperature)', 'AverageTemperature')

    window = Window.partitionBy('lat', 'long').orderBy('year')
    for i in range(1, 11):
        location_annual_df = location_annual_df.withColumn('lag%s' % i,
                            functions.lag(location_annual_df['AverageTemperature'], i).over(window))
    location_annual_df = location_annual_df.filter(~location_annual_df['lag10'].isNull())

    flood_df = spark.read.option("header", "true").csv("s3://climate-data-732/FloodArchiveCleaned.csv")
    flood_df = flood_df.withColumn('lat', functions.round(flood_df['lat'], round_digit))
    flood_df = flood_df.withColumn('long', functions.round(flood_df['long'], round_digit))
    flood_df = flood_df.withColumn('year', flood_df['Began'].substr(0, 4))
    pos_df = flood_df.select('lat', 'long', 'Year')
    pos_df = pos_df.withColumn('label', functions.lit(1))

    joined_df = location_annual_df.join(pos_df, on=['long', 'lat', 'year'], how='left')
    joined_df = joined_df.fillna(0, 'label')
    neg_proportion = round(
        joined_df.filter(joined_df['label'] == 1).count() / joined_df.filter(joined_df['label'] == 0).count(), 4)
    sampled = joined_df.sampleBy('label', fractions={0: neg_proportion, 1: 1}, seed=1000)

    train, test = sampled.randomSplit([0.75, 0.25])
    assembler = VectorAssembler(outputCol="features")
    assembler.setInputCols([i for i in sampled.columns if i.startswith('lag')])
    gbt = GBTClassifier(featuresCol="features", labelCol="label", predictionCol="prediction", maxIter=100, maxDepth=2,
                        seed=42, minInstancesPerNode=2)
    pipeline = Pipeline(stages=[assembler, gbt])
    model = pipeline.fit(train)
    predictions = model.transform(test)

    matrix_count = predictions.groupby('prediction', 'label').count()
    class_count = predictions.groupby('label').count().withColumnRenamed('count', 'class_count')
    matrix_count = matrix_count.join(class_count, on='label')
    matrix_count = matrix_count.withColumn('proportion', matrix_count['count'] / matrix_count['class_count'])
    p_matrix = matrix_count.toPandas()
    engine = create_engine(
        'postgresql+psycopg2://postgres:Aws_2020@database-1.cwfbooless1u.us-east-1.rds.amazonaws.com:5432/postgres')
    p_matrix.to_sql('graph_3_2', engine, if_exists='replace')
    model.write().overwrite().save("s3://climate-data-732/bcmodel_by_temperature")

