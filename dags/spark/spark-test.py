import pyspark
from operator import add
sc = pyspark.SparkContext()

data = sc.parallelize(list("Hello World"))
counts = data.map(lambda x:
    (x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
     ascending=False).collect()

for (word, count) in counts:
    print("{}: {}".format(word, count))