# from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("test").getOrCreate()
# print(spark.version)
# spark.stop()


from pyspark import SparkContext
print(SparkContext.getOrCreate().version)  # Spark version
print(SparkContext.getOrCreate()._jvm.scala.util.Properties.versionString())  # Scala version
