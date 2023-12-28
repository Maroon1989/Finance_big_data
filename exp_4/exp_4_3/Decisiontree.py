from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, mean, stddev
from tqdm import tqdm
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# read in data
spark = SparkSession.builder.appName("DecisionTree").getOrCreate()
# df = spark.read.text("iris.data")
df_train = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/train.csv', header=True, inferSchema=True)
df_test = spark.read.csv(r'/home/hadoop/Workspace/exp_3/input/test_original.csv', header=True, inferSchema=True)

'''
    For some reason, the read function
    did not separate the columns, so this
    was done manually.
'''
'''
'AMT_INCOME_TOTAL', 'NAME_EDUCATION_TYPE', 'AMT_CREDIT',
       'NAME_HOUSING_TYPE', 'NAME_FAMILY_STATUS', 'NAME_INCOME_TYPE',
       'NAME_CONTRACT_TYPE', 'REGION_POPULATION_RELATIVE', 'DAYS_BIRTH',
       'DAYS_EMPLOYED', 'DAYS_REGISTRATION', 'DAYS_ID_PUBLISH',
       'WEEKDAY_APPR_PROCESS_START', 'HOUR_APPR_PROCESS_START',
       'ORGANIZATION_TYPE','TARGET'
'''
df_train = df_train.withColumn('AMT_INCOME_TOTAL', col('AMT_INCOME_TOTAL'))\
       .withColumn('NAME_EDUCATION_TYPE',col('NAME_EDUCATION_TYPE'))\
       .withColumn('AMT_CREDIT',col('AMT_CREDIT'))\
       .withColumn('NAME_HOUSING_TYPE',col('NAME_HOUSING_TYPE'))\
       .withColumn('NAME_FAMILY_STATUS',col('NAME_FAMILY_STATUS'))\
       .withColumn('NAME_INCOME_TYPE',col('NAME_INCOME_TYPE'))\
       .withColumn('NAME_CONTRACT_TYPE',col('NAME_CONTRACT_TYPE'))\
       .withColumn('REGION_POPULATION_RELATIVE',col('REGION_POPULATION_RELATIVE'))\
       .withColumn('DAYS_BIRTH',col('DAYS_BIRTH'))\
       .withColumn('DAYS_EMPLOYED',col('DAYS_EMPLOYED'))\
       .withColumn('DAYS_REGISTRATION',col('DAYS_REGISTRATION'))\
       .withColumn('DAYS_ID_PUBLISH',col('DAYS_ID_PUBLISH'))\
       .withColumn('WEEKDAY_APPR_PROCESS_START',col('WEEKDAY_APPR_PROCESS_START'))\
       .withColumn('HOUR_APPR_PROCESS_START',col('HOUR_APPR_PROCESS_START'))\
       .withColumn('ORGANIZATION_TYPE',col('ORGANIZATION_TYPE'))\
       .withColumn('TARGET',col('TARGET'))
       
df_test = df_test.withColumn('AMT_INCOME_TOTAL', col('AMT_INCOME_TOTAL'))\
       .withColumn('NAME_EDUCATION_TYPE',col('NAME_EDUCATION_TYPE'))\
       .withColumn('AMT_CREDIT',col('AMT_CREDIT'))\
       .withColumn('NAME_HOUSING_TYPE',col('NAME_HOUSING_TYPE'))\
       .withColumn('NAME_FAMILY_STATUS',col('NAME_FAMILY_STATUS'))\
       .withColumn('NAME_INCOME_TYPE',col('NAME_INCOME_TYPE'))\
       .withColumn('NAME_CONTRACT_TYPE',col('NAME_CONTRACT_TYPE'))\
       .withColumn('REGION_POPULATION_RELATIVE',col('REGION_POPULATION_RELATIVE'))\
       .withColumn('DAYS_BIRTH',col('DAYS_BIRTH'))\
       .withColumn('DAYS_EMPLOYED',col('DAYS_EMPLOYED'))\
       .withColumn('DAYS_REGISTRATION',col('DAYS_REGISTRATION'))\
       .withColumn('DAYS_ID_PUBLISH',col('DAYS_ID_PUBLISH'))\
       .withColumn('WEEKDAY_APPR_PROCESS_START',col('WEEKDAY_APPR_PROCESS_START'))\
       .withColumn('HOUR_APPR_PROCESS_START',col('HOUR_APPR_PROCESS_START'))\
       .withColumn('ORGANIZATION_TYPE',col('ORGANIZATION_TYPE'))\
       .withColumn('TARGET',col('TARGET'))

feature_cols = df_train.columns[:-1]  # 假设最后一列是标签列
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_data = vector_assembler.transform(df_train)
test_data = vector_assembler.transform(df_test)

bst = DecisionTreeClassifier(featuresCol="features", labelCol="TARGET",maxDepth=5)
bst_model =bst.fit(train_data)
predictions = bst_model.transform(test_data)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="TARGET", metricName="f1")
# 计算 F1 分数
f1_score = evaluator.evaluate(predictions)
print("F1 Score:", f1_score)
evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="TARGET", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("accuracy:", accuracy)
# evaluator = MulticlassClassificationEvaluator(predictionCol="prediction", labelCol="TARGET", metricName="precision")
# precision = evaluator.evaluate(predictions)
# print("precision:", precision)



