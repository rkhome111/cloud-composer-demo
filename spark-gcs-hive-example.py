from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id
import os
import sys

if __name__ == "__main__":
    n = int(len(sys.argv))

    for _ in range(n):
        print('-----------------------------------',sys.argv[_],'------------',n)

    spark = SparkSession.builder \
        .appName("SparkSQLDemo") \
        .master("local[1]") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("CREATE DATABASE IF NOT EXISTS myDB")
    personRecordParquetDF = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("gs://rkboss-spark-test-data/person.csv")
#    myParquetDF = spark.read.parquet("gs://rkboss-spark-test-data/test")
    personRecordParquetDF.show()
    personRecordParquetDF.printSchema()
    personRecordParquetDF.write.mode("overwrite").saveAsTable("flight_data_tbl")

    print("getting data --------------------------")
    print(spark.sql("select count(*) from flight_data_tbl"))
    print(spark.catalog.listTables("myDB"))