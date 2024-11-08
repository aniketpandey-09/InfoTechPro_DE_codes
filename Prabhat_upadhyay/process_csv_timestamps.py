from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp
from pyspark.sql.types import StringType
import random

# Initialize Spark session
spark = SparkSession.builder.appName("CSV_Timestamp_Processing").getOrCreate()

# Load CSV file into a DataFrame
df = spark.read.csv("/FileStore/tables/Sales_SalesOrderDetail-1.csv", header=True, inferSchema=True)

# Show the initial DataFrame
df.show(truncate=False)

# Define the random time generation function
def random_time():
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return f"{hour:02}:{minute:02}:{second:02}"

# Use backticks to reference the column name correctly
date_column = "`2011-05-31 00:00:00.000`"

# Apply the random time generation function to the date_column
df = df.withColumn("updated_date_column", 
                   F.concat(F.substring(col(date_column), 1, 10), 
                            F.lit(" "), 
                            F.udf(random_time, StringType())()))

# Convert the updated date column to timestamp format
df = df.withColumn("updated_date_column", to_timestamp(col("updated_date_column")))

# Add a new column for UTC (assuming updated_date_column is in UTC)
df = df.withColumn("UTC_time", col("updated_date_column"))

# Add a new column for IST (convert UTC to IST, IST is UTC+5:30)
df = df.withColumn("IST_time", from_utc_timestamp(col("updated_date_column"), "Asia/Kolkata"))

# Show the result with updated random times and converted UTC/IST times
df.select("updated_date_column", "UTC_time", "IST_time").show(truncate=False)

# Stop Spark session
spark.stop()
