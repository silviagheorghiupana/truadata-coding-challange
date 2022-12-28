from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Silvia-session').getOrCreate()
parDF1=spark.read.parquet("data/sf-airbnb-clean.parquet")
parDF1.printSchema()
df01 = parDF1.select('price')
df01.agg({'price': 'min','price': 'max'}).show()
df02 = parDF1.filter(parDF1.price>5000).filter(parDF1.review_scores_value==10).agg({'bathrooms':'avg','bedrooms':'avg'})
df02.show()
+-------------+--------------+
|avg(bedrooms)|avg(bathrooms)|
+-------------+--------------+
|          3.0|         2.375|
+-------------+--------------+

from pyspark.sql.functions import col
df03 = df02.select(col("avg(bathrooms)").alias("avg_bathrooms"), col("avg(bedrooms)").alias("avg_bedrooms"))
df03.show()
+-------------+------------+
|avg_bathrooms|avg_bedrooms|
+-------------+------------+
|        2.375|         3.0|
+-------------+------------+
df03.write.options(header='True', delimiter=',').csv("out/out_2_3.txt")
parDF2 = parDF1.agg({'price': 'min','review_scores_rating': 'max'})
parDF2.show()
+-------------------------+----------+
|max(review_scores_rating)|min(price)|
+-------------------------+----------+
|                    100.0|      10.0|
+-------------------------+----------+

df04 = parDF1.join(parDF2, (parDF1["price"]==parDF2["min(price)"]) & (parDF1["review_scores_rating"]==parDF2["max(review_scores_rating)"]) ).select(col("accommodates"))
df04.write.options(header='False', delimiter=',').csv("out/out_2_4.txt")