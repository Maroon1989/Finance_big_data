from pyspark.sql import SparkSession
from pyspark.sql.functions import col, floor

# Initialize a Spark session
spark = SparkSession.builder.appName("CreditAmountDistribution").getOrCreate()

# Load the data
df = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/application_data.csv', header=True, inferSchema=True)

# Define the range step for loan amounts
range_step = 10000

# Create a new column for the loan amount range
df = df.withColumn('AMT_CREDIT_RANGE', (floor(col('AMT_CREDIT') / range_step) * range_step))

# Group by the range and count the occurrences
distribution = df.groupBy('AMT_CREDIT_RANGE').count()

# Shift the range by one step for the upper bound
distribution = distribution.select(
    col('AMT_CREDIT_RANGE').alias('RANGE_START'),
    (col('AMT_CREDIT_RANGE') + range_step).alias('RANGE_END'),
    col('count').alias('RECORD_COUNT')
).orderBy('RANGE_START')

# Show the distribution
distribution.show()
distribution_list = distribution.collect()

# 指定输出文件路径
output_file_path = 'exp_4_1_1_output.txt'

# 打开文件并写入数据
with open(output_file_path, 'w') as file:
    for row in distribution_list:
        file.write(f"{(row['RANGE_START'], row['RANGE_END'])}, {row['RECORD_COUNT']}\n")

print(f"Data has been written to {output_file_path}")
# Stop the session
spark.stop()