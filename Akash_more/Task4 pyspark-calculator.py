# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


spark = SparkSession.builder.appName("Calculator").getOrCreate()


def add(x, y):
    return float(x) + float(y)

def subtract(x, y):
    return float(x) - float(y)

def multiply(x, y):
    return float(x) * float(y)

def divide(x, y):
    if y == 0:
        return None  
    return x / y

add_udf = udf(add, DoubleType())
subtract_udf = udf(subtract, DoubleType())
multiply_udf = udf(multiply, DoubleType())
divide_udf = udf(divide, DoubleType())

 
data = [(10, 5), (20, 0), (15, 3), (50, 25)]

columns = ["x", "y"]

df = spark.createDataFrame(data, columns)


df.show()

df_with_addition = df.withColumn("addition", add_udf(df["x"], df["y"]))

df_with_subtraction = df_with_addition.withColumn("subtraction", subtract_udf(df["x"], df["y"]))

df_with_multiplication = df_with_subtraction.withColumn("multiplication", multiply_udf(df["x"], df["y"]))

df_with_division = df_with_multiplication.withColumn("division", divide_udf(df["x"], df["y"]))




df_with_addition.show()
df_with_subtraction.show()
df_with_multiplication.show()
df_with_division.show()


