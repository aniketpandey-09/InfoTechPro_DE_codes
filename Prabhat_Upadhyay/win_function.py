# Import necessary modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, ntile, lag, lead, cume_dist, percent_rank

# Initialize Spark session
# This is the entry point to work with Spark DataFrames and SQL functions
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

# -----------------------------
# Sample Data
# -----------------------------

# Sample data representing Employees, Departments, and Salaries
data = [
    ("Alice", "Sales", 1000),
    ("Bob", "Sales", 1500),
    ("Charlie", "Sales", 1000),
    ("David", "HR", 1200),
    ("Eva", "HR", 2000),
    ("Frank", "HR", 1600),
    ("Grace", "IT", 3000),
    ("Hank", "IT", 3500),
    ("Ivy", "IT", 3000)
]

# Create DataFrame from the sample data
df = spark.createDataFrame(data, ["Employee", "Department", "Salary"])

# Show the original DataFrame
# This displays the employee, department, and salary details
df.show()

# -----------------------------
# Define Window Specification
# -----------------------------

# Create a window specification that defines how the window functions are calculated.
# In this case, we partition the data by 'Department' and order by 'Salary' in descending order.
window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())

# -----------------------------
# Apply Window Functions
# -----------------------------

# Apply various window functions to the DataFrame using the defined window specification
df_with_window = df.select(
    col("Employee"),
    col("Department"),
    col("Salary"),
    
    # row_number() gives a unique sequential number within a partition
    row_number().over(window_spec).alias("row_number"),
    
    # rank() gives the rank of a value within a partition, with gaps in case of ties
    rank().over(window_spec).alias("rank"),
    
    # dense_rank() gives the rank without gaps, even when there are ties
    dense_rank().over(window_spec).alias("dense_rank"),
    
    # ntile() distributes the rows into a specified number of buckets (in this case, 3 buckets)
    ntile(3).over(window_spec).alias("ntile"),
    
    # lag() returns the previous row's value in the window
    lag(col("Salary"), 1).over(window_spec).alias("lag"),
    
    # lead() returns the next row's value in the window
    lead(col("Salary"), 1).over(window_spec
