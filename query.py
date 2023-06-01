import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
spark = SparkSession.builder \
 .master("local[*]") \
 .appName('tepst') \
 .getOrCreate()
 
df = spark.read \
 .option("header", "true") \
 .option("inferSchema", "true") \
 .csv('vehicles.csv') 

df = df.dropna()

# Convert String to Integer Type
df = df.withColumn("year",df.year.cast(IntegerType()))
df = df.withColumn("price",df.price.cast(IntegerType()))
 

df = df.filter( (df.price  > 0) & (df.year  > 1960) & (df.manufacturer =='fiat') & (df.model.like ("%500%")) ) \



df =  df.select("manufacturer","model","year","price")
vectorAssembler = VectorAssembler(inputCols = ['year'], outputCol = 'features')
vhouse_df = vectorAssembler.transform(df)

splits = vhouse_df.randomSplit([0.6, 0.2, 0.2], seed=50)
train_df = splits[0]
test_df = splits[1]
vhouse_df.show(3)
df.show(5)


lr = LinearRegression(featuresCol = 'features', labelCol='price', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))


lr_predictions = lr_model.transform(test_df)
lr_predictions.show()
# predictions = lr_model.transform(test_df)
# predictions.select("prediction","price","features").show()

# for col in df.dtypes:\
#     print(col[0]+" , "+col[1])



 
