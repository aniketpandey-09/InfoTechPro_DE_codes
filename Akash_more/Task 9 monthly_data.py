# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, col, count, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName('Split_by_Month').getOrCreate()

# Define the schema
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

# Load the data with the schema
df = spark.read.format('csv').option('header', 'false').schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

# Add a month column to the DataFrame
df_with_month = df.withColumn('Month', month(col('OrderDate')))

# Group by month and aggregate
monthly_data = df_with_month.groupBy('Month').agg(
    count('SalesOrderID').alias('OrderCount'),
    sum('LineTotal').alias('TotalSales')
)

# Show the aggregated results
monthly_data.show()

