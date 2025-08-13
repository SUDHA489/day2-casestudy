from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

spark = SparkSession.builder \
    .appName("test_deequ") \
    .config("spark.jars", "file:///C:/Users/lakshmi.sudha/libs/deequ-1.0.1.jar") \
    .getOrCreate()

print("CheckLevel.Error object:", CheckLevel.Error)

data = spark.createDataFrame(
    [(1, 100), (2, 200), (3, 300)],
    ["transaction_id", "sales_amount"]
)

check = Check(spark, CheckLevel.Error, "simple test") \
    .isComplete("transaction_id") \
    .isNonNegative("sales_amount")

result = VerificationSuite(spark).onData(data).addCheck(check).run()

print("Verification status:", result.status)

spark.stop()
