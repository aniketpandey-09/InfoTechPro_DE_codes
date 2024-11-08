# Import necessary modules from PySpark
from pyspark.sql import SparkSession

# Initialize a Spark session
# This is the entry point to work with Spark
spark = SparkSession.builder.appName("FileHandlingExample").getOrCreate()

# -----------------------------
# Handling JSON Files
# -----------------------------

# Path to the input JSON file
json_data_path = "/FileStore/tables/data.json"

# Load JSON file into a DataFrame
# 'header=True' ensures that the first row of the file is treated as the header
json_df = spark.read.json(json_data_path)

# Show the contents of the DataFrame (optional)
json_df.show()

# Print the schema of the DataFrame to understand the structure of the data
json_df.printSchema()

# Save the DataFrame back to a new JSON file
# 'mode="overwrite"' ensures that the existing file will be overwritten
json_df.write.mode("overwrite").json("/FileStore/tables/data_output.json")

# -----------------------------
# Handling CSV Files
# -----------------------------

# Path to the input CSV file
csv_data_path = "/FileStore/tables/Advertising-4.csv"

# Load CSV file into a DataFrame
# 'header=True' ensures that the first row is treated as headers
# 'inferSchema=True' automatically infers the data types of columns
csv_df = spark.read.format("csv") \
                   .option("header", "true") \
                   .option("inferSchema", "true") \
                   .load(csv_data_path)

# Show the contents of the DataFrame (optional)
csv_df.show()

# Print the schema of the DataFrame to understand the structure of the data
csv_df.printSchema()

# Save the DataFrame as a Parquet file
# 'mode="overwrite"' ensures that existing data will be overwritten
parquet_output_path = "/FileStore/tables/Advertising.parquet"
csv_df.write.mode("overwrite").format("parquet").save(parquet_output_path)

# Save the DataFrame as a Delta table
# This can be useful for handling large datasets and supporting ACID transactions
delta_output_path = "/FileStore/tables/Advertising.delta"
csv_df.write.mode("overwrite").format("delta").save(delta_output_path)

# -----------------------------
# End of the Code
# -----------------------------

# Stop the Spark session when done
spark.stop()

# -------------------------------------------------
# Additional Notes:
# - You can modify file paths according to your environment.
# - Parquet and Delta formats are ideal for large-scale data handling.
# - Ensure that Delta Lake is configured in your Spark environment for Delta file operations.
# -------------------------------------------------
