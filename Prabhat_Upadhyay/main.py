import random
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_utc_timestamp, from_utc_timestamp, lit, dayofyear, when, month, year, unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

# Initialize the Spark session
spark = SparkSession.builder.appName("Timezone Conversion and Sales Data by Month").getOrCreate()

# Schema
schema = """ 
          SalesOrderID INT,
          SalesOrderDetailID INT,
          CarrierTrackingNumber STRING,
          OrderQty INT,
          ProductID INT,
          SpecialOfferID INT,
          UnitPrice DOUBLE,
          UnitPriceDiscount DOUBLE,
          LineTotal DOUBLE,
          rowguid STRING,
          ModifiedDate STRING
"""

# Load the CSV data
df = spark.read.format("csv").option("header", "true").schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail-2.csv")

# Convert 'ModifiedDate' to timestamp
df = df.withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))

# Define a Python function to generate random seconds
def generate_random_seconds():
    return random.randint(0, 3600)  # Generates random seconds between 0 and 3600 (1 hour)

# Register the UDF
generate_random_seconds_udf = udf(generate_random_seconds, IntegerType())

# Apply the UDF to add randomness to the London time
df_with_timezone = df.withColumn("London_Time", from_utc_timestamp(col("ModifiedDate"), "Europe/London")) \
    .withColumn("Random_Seconds", generate_random_seconds_udf())

# Convert London_Time to Unix timestamp, add random seconds, and convert back to timestamp
df_with_timezone = df_with_timezone.withColumn("London_Time", 
    from_unixtime(unix_timestamp(col("London_Time")) + col("Random_Seconds")).cast("timestamp"))

# Adjust other European time zones based on the new London time
df_with_timezone = df_with_timezone.withColumn("Berlin_Time", from_utc_timestamp(col("London_Time"), "Europe/Berlin")) \
    .withColumn("Paris_Time", from_utc_timestamp(col("London_Time"), "Europe/Paris")) \
    .withColumn("Madrid_Time", from_utc_timestamp(col("London_Time"), "Europe/Madrid")) \
    .withColumn("Rome_Time", from_utc_timestamp(col("London_Time"), "Europe/Rome"))

# Determine if daylight saving is on or off (assuming DST between day 60 and day 300 of the year)
df_with_timezone = df_with_timezone.withColumn("Day_Of_Year", dayofyear(col("ModifiedDate"))) \
    .withColumn("Is_DST_On", when((col("Day_Of_Year") >= 60) & (col("Day_Of_Year") <= 300), lit("Yes")).otherwise(lit("No")))

# Extract year and month from 'ModifiedDate'
df_with_timezone = df_with_timezone.withColumn("Year", year(col("ModifiedDate"))).withColumn("Month", month(col("ModifiedDate")))

# Get distinct years and months
distinct_years_months = df_with_timezone.select("Year", "Month").distinct().collect()

# Dictionary to store DataFrames for each month
monthly_dataframes = {}

# Loop through distinct years and months to divide and save data in DataFrames
for row in distinct_years_months:
    year_val = row["Year"]
    month_val = row["Month"]
    
    # Filter data for the specific year and month
    df_filtered = df_with_timezone.filter((col("Year") == year_val) & (col("Month") == month_val))
    
    # Display the filtered data for the current year and month
    print(f"Data for {year_val}-{month_val}")
    df_filtered.select("London_Time", "Berlin_Time", "Paris_Time", "Madrid_Time", "Rome_Time").show(truncate=False)
    
    # Store the filtered DataFrame in the dictionary with a key like '2024_01' for January 2024
    monthly_dataframes[f"{year_val}_{month_val:02d}"] = df_filtered

    # Optional: Save each month's data to a CSV (if needed)
    # output_path = f"/mnt/data/sales_data_{year_val}_{month_val}.csv"
    # df_filtered.write.csv(output_path, header=True)

# Example: Accessing the DataFrame for January 2024
df_jan_2024 = monthly_dataframes.get("2024_01")
if df_jan_2024:
    df_jan_2024.show(truncate=False)

# Access the DataFrame for June 2014
df_jun_2014 = monthly_dataframes.get("2014_06")

if df_jun_2014:
    # Select only the columns for time zones
    df_jun_2014_time_only = df_jun_2014.select("London_Time", "Berlin_Time", "Paris_Time", "Madrid_Time", "Rome_Time")
    
    # Convert to Pandas DataFrame for better tabular display
    df_jun_2014_time_only_pd = df_jun_2014_time_only.toPandas()
    
    # Print the DataFrame in tabular form
    print("Data for 2014-06")
    print(df_jun_2014_time_only_pd.to_string(index=False))  # Convert to string without row indices
else:
    print("No data for June 2014")

# Access the DataFrame for October 2012
df_oct_2012 = monthly_dataframes.get("2012_10")

if df_oct_2012:
    # Select only the columns for time zones
    df_oct_2012_time_only = df_oct_2012.select("London_Time", "Berlin_Time", "Paris_Time", "Madrid_Time", "Rome_Time")
    
    # Convert to Pandas DataFrame for better tabular display
    df_oct_2012_time_only_pd = df_oct_2012_time_only.toPandas()
    
    # Print the DataFrame in tabular form
    print("Data for 2012-10")
    print(df_oct_2012_time_only_pd.to_string(index=False))  # Convert to string without row indices
else:
    print("No data for October 2012")
