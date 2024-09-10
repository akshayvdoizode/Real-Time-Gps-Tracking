from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType, DoubleType
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import from_json,col
import os 
# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29093')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'VEHICLE_TOPIC')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'GPS_TOPIC')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'TRAFFIC_TOPIC')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'WEATHER_TOPIC')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'EMERGENCY_TOPIC')
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
    # print(vehicleDF,"vehicleDF")
    streamWriter(vehicleDF,'s3://saprk-streaming-data-avd/checkpoints/vehicle_data','s3://saprk-streaming-data-avd/data/vehicle_data')
    streamWriter(gpsDF,'s3://saprk-streaming-data-avd/checkpoints/gps_data','s3://saprk-streaming-data-avd/data/gps_data')
    streamWriter(trafficDF,'s3://saprk-streaming-data-avd/checkpoints/traffic_data','s3://saprk-streaming-data-avd/data/traffic_data')
    streamWriter(weatherDF,'s3://saprk-streaming-data-avd/checkpoints/weather_data','s3://saprk-streaming-data-avd/data/weather_data')
    streamWriter(emergencyDF,'s3://saprk-streaming-data-avd/checkpoints/emergency_data','s3://saprk-streaming-data-avd/data/emergency_data')
    # query1=streamWriter(vehicleDF, './parquet/checkpoints/vehicle_data', './parquet/data/vehicle_data')
    # query2 =streamWriter(gpsDF, './parquet/checkpoints/gps_data', './parquet/data/gps_data')
    # query3=streamWriter(trafficDF, './parquet/checkpoints/traffic_data', './parquet/data/traffic_data')
    # query4=streamWriter(weatherDF, './parquet/checkpoints/weather_data', './parquet/data/weather_data')
    # query5=streamWriter(emergencyDF, './parquet/checkpoints/emergency_data', './parquet/data/emergency_data')
    # query5.awaitTermination()
    

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
    
    
    
    
  
    
# docker exec -it   codespaces-blank-spark-master-1 spark-submit \
# --master spark://spark-master:7077 \
# --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
# jobs/journey.py
# org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0


# docker exec -it   codespaces-blank-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/journey.py
# docker exec -it codespaces-blank-spark-master-1 spark-submit \
# --master spark://spark-master:7077 \
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
# jobs/journey.py








    # Writing to folders in the same directory where the code exists

    
