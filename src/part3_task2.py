from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Silvia-session-ml').getOrCreate()
raw_data=spark.read.csv("iris.csv",inferSchema=True)
raw_data.printSchema()

from pyspark.ml.feature import StringIndexer
indexer = StringIndexer(inputCol="_c4", outputCol="class_label")
indexed=indexer.fit(raw_data).transform(raw_data)
indexed.groupBy("class_label").count().show()


from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=["_c0","_c1","_c2","_c3"], outputCol="features")
output=assembler.transform(indexed)
output.show()


# mapping_dict = {"Iris-setosa":"0","Iris-versicolor":"1","Iris-virginica":"2"}
# print(mapping_dict)

data = output.select("features","class_label")
data.show()


training_df,test_df = data.randomSplit([0.8, 0.2])

from pyspark.ml.classification import  LogisticRegression
log_rec = LogisticRegression(labelCol="class_label").fit(training_df)
results = log_rec.evaluate(test_df).predictions
results.show()
pred_data_raw = spark.createDataFrame([(5.1, 3.5, 1.4, 0.2),(6.2, 3.4, 5.4, 2.3)],["sepal_length","sepal_width","petal_length","petal_width"])
pred_data_raw.show()
assembler2 = VectorAssembler(inputCols=["sepal_length","sepal_width","petal_length","petal_width"], outputCol="features")
pred_data=assembler2.transform(pred_data_raw).select("features")
pred_data.show()

from pyspark.sql.functions import lit
pred_data=assembler2.transform(pred_data_raw).select("features").withColumn("class_label",lit(7))
pred_data.show()

my_predictions = log_rec.evaluate(pred_data).predictions
my_predictions.show()

final_df = my_predictions.select("prediction").alias("class").repartition(1)
final_df.write.options(header='True').csv("out/out_3_2.txt")