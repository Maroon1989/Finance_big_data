from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, count

# Initialize a Spark session
spark = SparkSession.builder.appName("exp_4_2_1").getOrCreate()

# Load the data
df = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/application_data.csv', header=True, inferSchema=True)

# Create a new column for the loan amount range
df = df.withColumn('CODE_GENDER', col('CODE_GENDER')).withColumn('CNT_CHILDREN', col('CNT_CHILDREN'))

# Filter for CODE_GENDER == 'm' and group by CNT_CHILDREN
distribution = df.filter(col('CODE_GENDER') == 'M').groupBy(col('CNT_CHILDREN')).agg(count(col('CODE_GENDER')).alias('SUM_GENDER'))

# Calculate TYPE_RATIO using a subquery
distribution = distribution.select(
    col('CNT_CHILDREN'),
    (col('SUM_GENDER') / df.filter(col('CODE_GENDER') == 'M').count()).cast('string').alias('TYPE_RATIO')
)

# Show the distribution
distribution.show()
rows = distribution.collect()
# Write the results to a text file
output_file_path = 'exp_4_2_1_output.txt'
with open(output_file_path, 'w') as file:
    for row in rows:
        file.write(f"{row['CNT_CHILDREN'], row['TYPE_RATIO']}\n")
print(f"Data has been written to {output_file_path}")

# Stop the Spark session
spark.stop()
