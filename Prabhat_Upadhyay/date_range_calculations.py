from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, date_sub, date_format, to_date, col

# Initialize Spark session
spark = SparkSession.builder.appName("DateRangeCalculations").getOrCreate()

# Define date range start and end
date_range_start = '2023-09-01 12:00:00'  # Example value
date_range_end = '2023-09-01 13:00:00'    # Example value

# Convert string date ranges to date format
date_range_start_col = to_date(lit(date_range_start), 'yyyy-MM-dd HH:mm:ss')
date_range_end_col = to_date(lit(date_range_end), 'yyyy-MM-dd HH:mm:ss')

# Calculate last year's date range
last_year_start = spark.sql(f"""
SELECT date_sub(to_date('{date_range_start}', 'yyyy-MM-dd HH:mm:ss'), 365) AS last_year_start
""").collect()[0]['last_year_start']

last_year_end = spark.sql(f"""
SELECT date_sub(to_date('{date_range_end}', 'yyyy-MM-dd HH:mm:ss'), 365) AS last_year_end
""").collect()[0]['last_year_end']

# Create a DataFrame with the last year's dates
df = spark.createDataFrame([(last_year_start,), (last_year_end,)], ["last_year_date"])

# Extract date and year components
df = df.withColumn("date_component", date_format(col("last_year_date"), "yyyy-MM-dd"))
df = df.withColumn("year_component", year(col("last_year_date")))

# Display the result
df.show()

# Stop Spark session
spark.stop()
