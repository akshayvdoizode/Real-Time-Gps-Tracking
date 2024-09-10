from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType, DoubleType
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import from_json,col
from constants import * 
# spark = SparkSession.builder.appName("SmartCityStreaming")\
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,",
#                 "org.apache.hadoop:hadoop-aws:3.3.1,",
#                 "com.amazonaws:aws-java-sdk:1.11.469")\
#         .getOrCreate() 


# spark = SparkSession.builder.appName("SmartCityStreaming")
#     .config("spark.jars.packages",
#         "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
#         "org.apache.hadoop:hadoop-aws:3.3.1,"
#         "com.amazonaws:aws-java-sdk:1.11.469")\
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
#     .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY'))\
#     .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
#     .config("spark.hadoop.fs.s3a.aws.credentials.provider", 'org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentialsProvider')\
#     .getOrCreate()




spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                                   "org.apache.hadoop:hadoop-aws:3.3.1,"
                                   "com.amazonaws:aws-java-sdk:1.11.469")\
    .getOrCreate()
filepath=''


def main():
    # Define the schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),  # UUID as a string
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),  # UUID as a string
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True)
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),  # UUID as a string
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),  # UUID as a string
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("AQI", DoubleType(), True)
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),  # UUID as a string
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    vehicleDF=read_kafka_topic(VEHICLE_TOPIC,vehicleSchema).alias("vehicle")
    gpsDF = read_kafka_topic(GPS_TOPIC, gpsSchema).alias('gps')
    trafficDF = read_kafka_topic(TRAFFIC_TOPIC, trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic(WEATHER_TOPIC, weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic(EMERGENCY_TOPIC, emergencySchema).alias('emergency')
    
    query1=streamWriter(vehicleDF, filepath,filepath)
    query2 =streamWriter(gpsDF, filepath,filepath)
    query3=streamWriter(trafficDF, filepath,filepath)
    query4=streamWriter(weatherDF, filepath,filepath)
    query5=streamWriter(emergencyDF, filepath,filepath)
    query5.awaitTermination()
    

def read_kafka_topic(topic, schema):
    return spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'broker:29092') \
        .option('subscribe', topic) \
        .option('startingOffsets', 'earliest') \
        .option("failOnDataLoss", "false") \
        .load() \
        .selectExpr('CAST(value AS STRING)') \
        .select(from_json(col('value'), schema).alias('data')) \
        .select('data.*') \
        .withWatermark('timestamp', '2 minutes')


def streamWriter(input: DataFrame, checkpointFolder, output):
    return input.writeStream \
        .format('parquet') \
        .option('checkpointLocation', checkpointFolder) \
        .option('path', output) \
        .outputMode('append') \
        .start()


if __name__=="__main__":
    main()
    
    
    
    
    
    
