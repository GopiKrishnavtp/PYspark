from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf

conf = SparkConf().setAppName("first").setMaster("local[*]")
sc=SparkContext(conf=conf)
spark=SparkSession.builder.getOrCreate()
sc.textFile("file:///D:\cust.text").foreach(print)