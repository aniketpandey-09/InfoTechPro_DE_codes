# Databricks notebook source


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("example").getOrCreate()

df = spark.read.format("csv").option("header","true").option("inferschema","true").load("/FileStore/tables/star_dataset-6.csv")

df.show()

df.printSchema()

df.write.mode("overwrite").format("parquet").save("/FileStore/tables/star_dataset1.csv")
df.write.mode("overwrite").format("delta").save("/FileStore/tables/star_dataset2.csv")

