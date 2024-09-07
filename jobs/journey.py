from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, LongType, TimestampType,DoubleType

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

gpsSchema=StructType([
    StructField("id", StringType(), True),  # UUID as a string
    StructField("deviceId", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("speed", DoubleType(), True),
    StructField("direction", StringType(), True),
    StructField("vehicleType", StringType(), True)
])


trafficSchema=StructType([
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
   
emergencySchema=StructType([
    StructField("id", StringType(), True),  # UUID as a string
    StructField("deviceId", StringType(), True),
    StructField("incidentId", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", TimestampType(), True),
])
