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
# lk=[1,2,3,4]
# rddlk=sc.parallelize(lk)
# print(rddlk.collect())
# addlk=rddlk.map(lambda x:x+2)
# print(addlk.collect())
# mullk=rddlk.map(lambda x:x*2)
# print(mullk.collect())
# lkstr=["gopi","gopiking","king"]
# rddklstr=sc.parallelize(lkstr)
# print(rddklstr.collect())
# conlk=rddklstr.map(lambda x : x+"analytics")
# print(conlk.collect())
# replk=rddklstr.map(lambda x :x.replace("gopi","tiger"))
# print(replk.collect())
# fillk=rddklstr.filter(lambda x:'king' in x)
# print(fillk.collect())
# mlk=["A~B","C~D"]
# rddmlk=sc.parallelize(mlk)
# rddmlk.foreach(print)
# splmlk=rddmlk.flatMap(lambda x: x.split("~"))
# print(splmlk.collect())
# rddtxt=sc.textFile("state.txt")
# rddtxt.foreach(print)
# fltdata=rddtxt.flatMap(lambda x :x.split(","))
# fltdata.foreach(print)
# stadata=fltdata.filter(lambda x : 'State' in x)
# stadata.foreach(print)
# fista=stadata.map(lambda x : x.replace("State->",""))
# fista.foreach(print)
# citdata=fltdata.filter(lambda x : 'City' in x)
# citdata.foreach(print)
# ficit=citdata.map(lambda x : x.replace("City->",""))
# ficit.foreach(print)
# rddus=sc.textFile("usdata.csv")
# rddus.foreach(print)
# lenrdd=rddus.filter(lambda x : len(x)>200)
# lenrdd.foreach(print)
# flrdd=lenrdd.flatMap(lambda x :x.split(","))
# flrdd.foreach(print)
# reprdd=flrdd.map(lambda x:x.replace("-",""))
# reprdd.foreach(print)
# det=sc.textFile("dt.txt")
# det.foreach(print)
# mpsplit=det.map(lambda x : x.split(","))
# mpsplit.foreach(print)
#
# from collections import namedtuple
# columns=namedtuple("columns",['id','tdate','amt','category','product','spendby'])
# asscol=mpsplit.map(lambda x:columns(x[0],x[1],x[2],x[3],x[4],x[5]))
#
# proofil=asscol.filter(lambda x:'Gymnastics' in x.product)
# proofil.foreach(print)
#
# prodf=proofil.toDF()
# prodf.show()
# prodf.createOrReplaceTempView("BGT")
# spark.sql("select id,tdate from BGT").show()
# csvvdf=spark.read.format("csv").option("header","true").load("df.csv")
# csvvdf.show()
data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]


df = spark.createDataFrame(data).toDF("tno", "tdate", "amount", "category", "product", "spendby")

df.show()
# selldf=df.select("tno","tdate")
# selldf.show()
# sinfil=df.filter("category='Exercise'")
# sinfil.show()
# mulsin=df.filter("category='Exercise' and spendby='cash'")
# mulsin.show()
# mulorsin=df.filter("category='Exercise' or spendby='cash'")
# mulorsin.show()
# mulcolsin=df.filter("category in ('Exercise','Gymnastics')")
# mulcolsin.show()
# proofil=df.filter("product like '%Gymnastics%'")
# proofil.show()
# nuullfil=df.filter("category!='Exercise'")
# nuullfil.show()
# nugfil=df.filter("product is null")
# nugfil.show()
# nuggfil=df.filter("product is not null")
# nuggfil.show()
from pyspark.sql.functions import *
# prolif=df.selectExpr("cast(tno as int) as tno",
#                      "split(tdate,'-')[2] as tdate",
#                      "amount+100 as amount",
#                      "upper(category) as category",
#                      "concat(product,'-zeyo') as product",
#                      "spendby",
#                      "case when spendby = 'cash' then 1 else 0 end as status"
#
#
#
#
#
#
#                      )
# prolif.show()
wittt=df.withColumn("category",expr("upper(category)"))
wittt.show()