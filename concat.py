from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("ParentChildTable").getOrCreate()

# Configuration of BigQuery parameters (replace with actual values)
project_id = "your_project_id"
parent_table = "your_dataset.your_parent_table"  # BigQuery parent table
child_table_prefix = "your_dataset.child_table_"  # Prefix for child tables in BigQuery
bucket = "your-gcs-bucket"  # GCS bucket to use with BigQuery connector

# Read parent table from BigQuery
parent_df = spark.read \
    .format("bigquery") \
    .option("parentProject", project_id) \
    .option("table", parent_table) \
    .load()

# Configuration of child tables
child_table_columns = {
    "child_0": ["column0", "column1", "column2", "column3"],
    "child_1": ["column8", "column9", "column10", "column11", "column12"],
    # Add more child tables as needed
}

# Function to read child table based on configuration
def process_child_tables(parent_df):
    for child_table_name, selected_columns in child_table_columns.items():
        # Select the relevant columns for the child table
        child_df = parent_df.select(*selected_columns)
        
        # Write the child table to BigQuery
        child_table = f"{child_table_prefix}{child_table_name}"
        child_df.write \
            .format("bigquery") \
            .option("parentProject", project_id) \
            .option("table", child_table) \
            .option("temporaryGcsBucket", bucket) \
            .mode("overwrite") \
            .save()

# Process all child tables
process_child_tables(parent_df)
