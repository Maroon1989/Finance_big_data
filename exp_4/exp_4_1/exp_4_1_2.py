from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor,count

# Initialize a Spark session
spark = SparkSession.builder.appName("Minus").getOrCreate()

# Load the data
df = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/application_data.csv', header=True, inferSchema=True)

# # Define the range step for loan amounts
# range_step = 10000

# Create a new column for the loan amount range
# df = df.withColumn('AMT_CREDIT_RANGE', (floor(col('AMT_CREDIT') / range_step) * range_step))
df = df.withColumn('SK_ID_CURR',col('SK_ID_CURR')).withColumn('AMT_CREDIT',col('AMT_CREDIT')).withColumn('AMT_INCOME_TOTAL',col('AMT_INCOME_TOTAL'))
# Group by the range and count the occurrences
# distribution = df.groupBy('AMT_CREDIT_RANGE').count()
distribution = df
# Shift the range by one step for the upper bound
distribution = distribution.select(
    col('SK_ID_CURR').alias('SK_ID_CURR'),
    col('NAME_CONTRACT_TYPE').alias('NAME_CONTRACT_TYPE'),
    col('AMT_CREDIT').alias('AMT_CREDIT'),
    col('AMT_INCOME_TOTAL').alias('AMT_INCOME_TOTAL'),
    (col('AMT_CREDIT') - col('AMT_INCOME_TOTAL')).alias('MINUS'),
    # col('count').alias('RECORD_COUNT')
).orderBy(col('MINUS').desc())

# Show the distribution
distribution.show()
top10 = distribution.limit(10).collect()
bottom10 = distribution.orderBy(col('MINUS')).limit(10).collect()

# 指定输出文件路径
output_file_path = 'exp_4_1_2_output.txt'

# 打开文件并写入数据
with open(output_file_path, 'w') as file:
    for row in top10:
        file.write(f"{row['SK_ID_CURR'], row['NAME_CONTRACT_TYPE'],row['AMT_CREDIT'],row['AMT_INCOME_TOTAL']}, {row['MINUS']}\n")
    file.write("\n")
    for row in bottom10:
        file.write(f"{row['SK_ID_CURR'], row['NAME_CONTRACT_TYPE'],row['AMT_CREDIT'],row['AMT_INCOME_TOTAL']}, {row['MINUS']}\n")

print(f"Data has been written to {output_file_path}")
# Stop the session
spark.stop()