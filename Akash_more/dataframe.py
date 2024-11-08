# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Load the CSV File First
# MAGIC # Read the CSV file into a DataFrame:
# MAGIC

# COMMAND ----------

csv_df = spark.read.csv("/FileStore/tables/SalesOrder.csv", header=True, inferSchema=True)
csv_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Save and Read in Other Formats
# MAGIC # 1. Convert to Parquet

# COMMAND ----------

csv_df.write.parquet("/FileStore/tables/SalesOrder.parquet")
parquet_df = spark.read.parquet("/FileStore/tables/SalesOrder.parquet")
parquet_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Convert to ORC

# COMMAND ----------

csv_df.write.orc("/FileStore/tables/SalesOrder.orc")
orc_df = spark.read.orc("/FileStore/tables/SalesOrder.orc")
orc_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Convert to JSON

# COMMAND ----------

csv_df.write.json("/FileStore/tables/SalesOrder.json")
json_df = spark.read.json("/FileStore/tables/SalesOrder.json")
json_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Convert to Avro

# COMMAND ----------

csv_df.write.format("avro").save("/FileStore/tables/SalesOrder.avro")
avro_df = spark.read.format("avro").load("/FileStore/tables/SalesOrder.avro")
avro_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Convert to Text

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# Concatenate columns with a delimiter (e.g., comma)
csv_df.select(concat_ws(",", csv_df.OrderID, csv_df.CustomerID, csv_df.Amount).alias("combined_text")) \
    .write.text("/FileStore/tables/SalesOrder.txt")

# Read the text file
text_df = spark.read.text("/FileStore/tables/SalesOrder.txt")
text_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Convert to Hive Table

# COMMAND ----------

csv_df.write.saveAsTable("default.SalesOrder")
hive_df = spark.sql("SELECT * FROM default.SalesOrder")
hive_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 7. Convert to Delta format

# COMMAND ----------

# Save DataFrame as Delta table
delta_path = "/FileStore/tables/SalesOrder_delta"
csv_df.write.format("delta").mode("overwrite").save(delta_path)


# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Convert to Binary

# COMMAND ----------

# Read back as a binary file
binary_df = spark.read.format("binaryFile").load("/FileStore/tables/SalesOrder.parquet")
binary_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. Convert to Image
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

# Load the Delta DataFrame
delta_path = "/FileStore/tables/SalesOrder_delta"
delta_df = spark.read.format("delta").load(delta_path)

# Convert Spark DataFrame to Pandas DataFrame (for plotting)
pandas_df = delta_df.toPandas()

# Example plot: Bar chart for OrderID and Amount
plt.figure(figsize=(10, 6))
plt.bar(pandas_df['OrderID'], pandas_df['Amount'], color='blue')

# Add titles and labels
plt.title('Sales Order Amounts')
plt.xlabel('OrderID')
plt.ylabel('Amount')

# Save the plot as an image
image_path = "/dbfs/FileStore/tables/SalesOrder_plot.png"
#plt.savefig(image_path)

# Display the image
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # 10. Convert to XML

# COMMAND ----------

import pandas as pd

# Read the CSV into a Pandas DataFrame
df = pd.read_csv("/FileStore/tables/SalesOrder-1.csv")


# COMMAND ----------


