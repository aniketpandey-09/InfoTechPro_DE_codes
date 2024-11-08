# Databricks notebook source
from pyspark.sql.types import *


data = [(1,'Maheer'),(2, 'Wafa')] 
schema = ['Id','Name']
df =spark.createDataFrame(data,schema)
df.show()
df.printSchema()



# COMMAND ----------

from pyspark.sql.types import *

#help(StructType)
#help(StructField)

data = [(1,'Maheer'),(2, 'Wafa')] 


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

df =spark.createDataFrame(data,schema)
df.show()
df.printSchema()

type(schema)


# COMMAND ----------

data = [{'id':1, 'name':'Maheer'},
        {'id':2, 'name':'wafa'}]

df = spark.createDataFrame(data)
df.show()
df.printSchema

