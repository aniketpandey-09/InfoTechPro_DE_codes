# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  to_utc_timestamp, from_utc_timestamp

#initialize the spark session

spark = SparkSession.builder.appName("Timezone Conversion").getOrCreate()


schema = """ 
          SalesOrderID INT,
          SalesOrderDetailID INT,
          CarrierTrackingNumber STRING,
          OrderQty INT,
          ProductID INT,
          SpecialOfferID INT,
          UnitPrice DOUBLE,
          UnitPriceDiscount DOUBLE,
          LineTotal Double,
          rowguid STRING,
          ModifiedDate STRING

"""

df = spark.read .format("csv").option("header", "true").schema(schema).load("/FileStore/tables/Sales_SalesOrderDetail.csv")

df.printSchema()
df.show()

df = df.withColumn("ModifiedDate", col("ModifiedDate").cast("timestamp"))


df_with_timezone = df.withColumn("UTC", to_utc_timestamp(col("ModifiedDate"), "UTC")) \
                      .withColumn("IST", from_utc_timestamp(col("ModifiedDate"), "Asia/Kolkata"))


df_with_timezone.show()

df_with_timezone.select("SalesOrderID", "SalesOrderDetailID", "ModifiedDate", "UTC", "IST").distinct().show()







