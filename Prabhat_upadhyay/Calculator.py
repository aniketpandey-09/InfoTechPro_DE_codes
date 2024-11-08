from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Step 1: Set Up Spark Session
spark = SparkSession.builder.appName("Calculator").getOrCreate()

# Step 2: Define Calculator Functions
def add(x, y):
    if x is not None and y is not None:
        return float(x) + float(y)
    return None

def subtract(x, y):
    if x is not None and y is not None:
        return float(x) - float(y)
    return None

def multiply(x, y):
    if x is not None and y is not None:
        return float(x) * float(y)
    return None

def divide(x, y):
    if x is not None and y is not None and y != 0:
        return float(x) / float(y)
    return None

# Register these functions as UDFs (User Defined Functions) in PySpark
add_udf = udf(add, DoubleType())
subtract_udf = udf(subtract, DoubleType())
multiply_udf = udf(multiply, DoubleType())
divide_udf = udf(divide, DoubleType())

# Step 3: Create a Sample DataFrame
data = [(10, 5), (20, 0), (15, 3), (50, 25)]
columns = ["x", "y"]
df = spark.createDataFrame(data, columns)

# Ensure the columns are of DoubleType
df = df.withColumn("x", df["x"].cast(DoubleType()))
df = df.withColumn("y", df["y"].cast(DoubleType()))

# Show the original DataFrame
df.show()

# Step 4: Apply Calculator Functions to the DataFrame
df_with_addition = df.withColumn("addition", add_udf(df["x"], df["y"]))
df_with_subtraction = df_with_addition.withColumn("subtraction", subtract_udf(df["x"], df["y"]))
df_with_multiplication = df_with_subtraction.withColumn("multiplication", multiply_udf(df["x"], df["y"]))
df_with_division = df_with_multiplication.withColumn("division", divide_udf(df["x"], df["y"]))

# Show the DataFrame with all operations
df_with_division.show()
