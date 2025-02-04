import os
import urllib.request
import ssl

data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

data_dir1 = "hadoop/bin"
os.makedirs(data_dir1, exist_ok=True)

urls_and_paths = {
    "https://raw.githubusercontent.com/saiadityaus1/SparkCore1/master/test.txt": os.path.join(data_dir, "test.txt"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir1, "winutils.exe"),
    "https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir1, "hadoop.dll")
}

# Create an unverified SSL context
ssl_context = ssl._create_unverified_context()

for url, path in urls_and_paths.items():
    # Use the unverified context with urlopen
    with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
        data = response.read()
        out_file.write(data)
import os, urllib.request, ssl; ssl_context = ssl._create_unverified_context(); [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in { "https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv", "https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv", "https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt", "https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt", "https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt", "https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt", "https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json", "https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet", "https://github.com/saiadityaus1/test1/raw/main/file6": "file6", "https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv", "https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt", "https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"}.items()]

# ======================================================================================

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import sys

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path

os.environ['JAVA_HOME'] = r'C:\Users\gopik\.jdks\corretto-1.8.0_432'

conf = SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host","localhost").set("spark.default.parallelism", "1")
sc = SparkContext(conf=conf)

spark = SparkSession.builder.getOrCreate()

#################🔴🔴🔴🔴🔴🔴 -> DONT TOUCH ABOVE CODE -- TYPE BELOW ####################################
## AGG


# data = [("sai", 40), ("zeyo", 30), ("sai", 50), ("zeyo", 40), ("sai", 10)]
#
# df = spark.createDataFrame(data, ["name", "amount"])
#
# df.show()
# df.printSchema()
#
# from  pyspark.sql.functions import *
#
# aggdf = (
#
#     df
#     .groupBy("name")
#     .agg(
#         sum("amount").alias("total") ,
#         count("name").alias("cnt")
#     )
#
# )
#
# aggdf.show()
#
#
#
#
# data1 = [("sai","chennai", 40), ("sai","hydb", 50), ("sai","chennai", 10), ("sai","hydb", 60)]
#
# df1 = spark.createDataFrame(data1, ["name", "location", "amount"])
#
# df1.show()
# df1.printSchema()
#
#
#
#
# from pyspark.sql.functions import *
#
# aggdf1 =(
#
#     df1.groupby("name","location")
#     .agg(sum("amount").alias("total"))
#
# )
#
# aggdf1.show()
# data = [("sai","chennai", 40), ("zeyo","hyd", 30), ("sai","chennai", 50), ("zeyo","hyd", 40), ("sai","hyd", 10)]
#
# df = spark.createDataFrame(data, ["name","location", "amount"])
# from pyspark.sql.functions import *
# df.show()
# df.printSchema()
# agdf=df.groupBy("name","location").agg(sum("amount").alias("total"),count("name").alias("cnt"))
# agdf.show()
# windowing  operation
from pyspark.sql.functions import  *


data = [("DEPT3", 500),
        ("DEPT3", 100),
        ("DEPT1", 1000),
        ("DEPT1", 700),
        ("DEPT1", 500),
        ("DEPT2", 400),
        ("DEPT2", 200)]
columns = ["dept", "salary"]
df = spark.createDataFrame(data, columns)
df.show()
from  pyspark.sql.window import Window
deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())

dfrank = df.withColumn("drank",dense_rank().over(deptwindow))
dfrank.show()
finaldf = dfrank.filter("drank=2").drop("drank")
finaldf.show()