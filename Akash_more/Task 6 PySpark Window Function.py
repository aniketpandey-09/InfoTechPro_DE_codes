# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, cume_dist, ntile

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()


# Sample data with 25 records
data = [
    (1, "Alice", "HR", 5000),
    (2, "Bob", "HR", 4500),
    (3, "Charlie", "IT", 7000),
    (4, "David", "IT", 7200),
    (5, "Eve", "IT", 6800),
    (6, "Frank", "HR", 4800),
    (7, "Grace", "IT", 6900),
    (8, "Hank", "IT", 7300),
    (9, "Ivy", "HR", 4700),
    (10, "Jack", "HR", 5100),
    (11, "Kim", "Finance", 6000),
    (12, "Lee", "Finance", 6200),
    (13, "Mia", "Finance", 5900),
    (14, "Nina", "Marketing", 5500),
    (15, "Oscar", "Marketing", 5700),
    (16, "Paul", "Marketing", 5400),
    (17, "Quinn", "Sales", 5300),
    (18, "Rita", "Sales", 5100),
    (19, "Sam", "Sales", 5500),
    (20, "Tina", "Sales", 5000),
    (21, "Uma", "Operations", 7000),
    (22, "Victor", "Operations", 7200),
    (23, "Wendy", "Operations", 6900),
    (24, "Xander", "Operations", 7100),
    (25, "Yara", "Operations", 6800)
]

columns = ["employee_id", "name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.show()

# define windowing function
windowSpec = Window.partitionBy("department").orderBy("salary")

# Row Number
df.withColumn("row_number", row_number().over(windowSpec)).show()


# Rank()

df.withColumn("rank", rank().over(windowSpec)).show()
 
# Dense_rank()

df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# Lag()

df.withColumn("prev_salary", lag("salary", 1).over(windowSpec)).show()

# Lead()

df.withColumn("next_salary", lead("salary", 1).over(windowSpec)).show()

#cume_dist()
df.withColumn("cume_dist", cume_dist().over(windowSpec)).show()

#ntile
df.withColumn("ntile", ntile(2).over(windowSpec)).show()



#1  FIRST() / LAST()

from pyspark.sql.functions import first, last

df.withColumn("first_salary", first("salary").over(windowSpec))\
  .withColumn("last_salary", last("salary").over(windowSpec))\
  .show(truncate=False)

# 2 SUM()

from pyspark.sql.functions import sum

df.withColumn("sum_salary", sum("salary").over(windowSpec))\
  .show(truncate=False)

#  3 AVG()

from pyspark.sql.functions import avg

df.withColumn("avg_salary", avg("salary").over(windowSpec))\
  .show(truncate=False)

# 4 STDDEV()

from pyspark.sql.functions import stddev

df.withColumn("stddev_salary", stddev("salary").over(windowSpec))\
  .show(truncate=False)

# 5 varience
from pyspark.sql.functions import variance

df.withColumn("variance_salary", variance("salary").over(windowSpec))\
  .show(truncate=False)

# 6 percent_rank
from pyspark.sql.functions import percent_rank

df.withColumn("percent_rank", percent_rank().over(windowSpec))\
  .show(truncate=False)

# 7 Ntile
df.withColumn("ntile", ntile(5).over(windowSpec))\
  .show(truncate=False)

# 8 PERCENTILE()
from pyspark.sql.functions import cume_dist

df.withColumn("cume_dist", cume_dist().over(windowSpec))\
  .show(truncate=False)


# 10 PERCENTILE_APPROX
from pyspark.sql.functions import percentile_approx

df.withColumn("percentile_50", percentile_approx("salary", 0.5).over(windowSpec))\
  .show(truncate=False)

