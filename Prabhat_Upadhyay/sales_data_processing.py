from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from pyspark.sql import functions as F
from pyspark.sql.functions import year, month

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("SalesDataProcessing").getOrCreate()

# Step 2: Define Schema for CSV File
schema = StructType([
    StructField("SalesOrderID", IntegerType(), True),
    StructField("SalesOrderDetailID", IntegerType(), True),
    StructField("CarrierTrackingNumber", StringType(), True),
    StructField("OrderQty", IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("SpecialOfferID", IntegerType(), True),
    StructField("UnitPrice", DecimalType(10, 2), True),
    StructField("UnitPriceDiscount", DecimalType(10, 2), True),
    StructField("LineTotal", DecimalType(20, 2), True),
    StructField("rowguid", StringType(), True),
    StructField("OrderDate", DateType(), True)
])

# Step 3: Load CSV Data into DataFrame
df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Step 4: Cast OrderDate to Timestamp and Extract Year and Month
df = df.withColumn("OrderDate", df["OrderDate"].cast("timestamp"))
df = df.withColumn("Year", year(df["OrderDate"]))
df = df.withColumn("Month", month(df["OrderDate"]))

# Step 5: Get Distinct Year-Month Combinations
distinct_months = df.select("Year", "Month").distinct().orderBy("Year", "Month").collect()

# Step 6: Process Data for Each Month
for row in distinct_months:
    year_val = row['Year']
    month_val = row['Month']
    
    # Filter Data for the Specific Year and Month
    monthly_df = df.filter((df["Year"] == year_val) & (df["Month"] == month_val))
    
    # Aggregate Total Order Quantity and Total Line Total
    aggregated_df = monthly_df.groupBy("Year", "Month").agg(
        F.sum("OrderQty").alias("TotalOrderQty"),
        F.sum("LineTotal").alias("TotalLineTotal")
    )
    
    # Print Aggregated Data for Current Month
    print(f"Aggregated data for {year_val}-{month_val:02d}:")
    aggregated_df.show()
    
    # Save DataFrame of Each Month into Separate CSV Files
    monthly_df.write.csv(f"/FileStore/tables/Sales_{year_val}_{month_val:02d}.csv")

# Final Aggregation (optional, last processed month)
aggregated_df = monthly_df.groupBy("Year", "Month").agg(
    F.sum("OrderQty").alias("TotalOrderQty"),
    F.sum("LineTotal").alias("TotalLineTotal")
)
print(f"Aggregated data for {year_val}-{month_val:02d}:")
aggregated_df.show()

# Save the final aggregated DataFrame (optional)
monthly_df.write.csv(f"/FileStore/tables/Sales_{year_val}_{month_val:02d}.csv")
