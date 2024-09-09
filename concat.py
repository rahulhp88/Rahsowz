from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("ParentChildTable").getOrCreate()

# Sample parent table schema (replace with actual data)
data = [(1, 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'aa', 'ab', 'ac')]
columns = [f"column{i}" for i in range(30)]

# Create DataFrame for parent table
parent_df = spark.createDataFrame(data, columns)

# Configuration of child tables
child_table_columns = {
    "child_0": ["column0", "column1", "column2", "column3"],
    "child_1": ["column8", "column9", "column10", "column11", "column12"],
    # Add more child tables as needed
}

# Function to read child table based on configuration
def read_child_table(parent_df, child_table_name):
    selected_columns = child_table_columns.get(child_table_name)
    if selected_columns:
        return parent_df.select(*selected_columns)
    else:
        print(f"Child table '{child_table_name}' not found in configuration")
        return None

# Example usage
child_0_df = read_child_table(parent_df, "child_0")
child_1_df = read_child_table(parent_df, "child_1")

# Show child tables
child_0_df.show()
child_1_df.show()
