from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor, count

# Initialize a Spark session
spark = SparkSession.builder.appName("exp_4_2_2").getOrCreate()

# Load the data
df = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/application_data.csv', header=True, inferSchema=True)

# Create a new column for the loan amount range
df = df.withColumn('SK_ID_CURR',col('SK_ID_CURR')).withColumn('AMT_INCOME_TOTAL', col('AMT_INCOME_TOTAL')).withColumn('DAYS_BIRTH',col('DAYS_BIRTH'))
distribution  = df.select(
    col('SK_ID_CURR'),
    (-col('AMT_INCOME_TOTAL')/col('DAYS_BIRTH')).alias('avg_income')
)
distribution.show()
distribution = distribution.filter(col('avg_income')>1).orderBy(col('avg_income').desc())
# # Filter for CODE_GENDER == 'm' and group by CNT_CHILDREN
# distribution = df.filter(col('CODE_GENDER') == 'M').groupBy(col('CNT_CHILDREN')).agg(count(col('CODE_GENDER')).alias('SUM_GENDER'))

# # Calculate TYPE_RATIO using a subquery
# distribution = distribution.select(
#     col('CNT_CHILDREN'),
#     (col('SUM_GENDER') / df.filter(col('CODE_GENDER') == 'M').count()).cast('string').alias('TYPE_RATIO')
# )

# Show the distribution
distribution.show()
rows = distribution.collect()
# Write the results to a text file
output_file_path = 'exp_4_2_2_output.csv'
with open(output_file_path, 'w') as file:
    for row in rows:
        file.write(f"{row['SK_ID_CURR'], row['avg_income']}\n")
print(f"Data has been written to {output_file_path}")

# Stop the Spark session
spark.stop()
