from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col

# Initialize Spark session
spark = SparkSession.builder.appName("ConcatColumns").getOrCreate()

# Sample DataFrame
data = [("John", "Doe", "12345"),
        ("Jane", "Smith", "67890")]

columns = ["FirstName", "LastName", "ID"]

df = spark.createDataFrame(data, columns)

def create_composite_pk(df, col1, col2=None):
    if col2:
        # Concatenate columns if both are provided
        return df.withColumn("CompositePK", concat(col(col1), lit("_"), col(col2)))
    else:
        # Use the single column as PK
        return df.withColumn("CompositePK", col(col1))

# Example usage with two columns
df_with_composite_pk = create_composite_pk(df, "FirstName", "LastName")
df_with_composite_pk.show()

# Example usage with one column
df_with_single_pk = create_composite_pk(df, "ID")
df_with_single_pk.show()
