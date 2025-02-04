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
# lists=[1,2,3,4,5]
# print(lists)
# rddlists=sc.parallelize(lists)
# print(rddlists.collect())
# addlists=rddlists.map(lambda x : x+2)
# print(addlists.collect())
# fillists=rddlists.filter(lambda x : x > 2)
# print(fillists.collect())
#
# srlists=["zeyo","zeyobron","gopi"]
# print(srlists)
# rddsr=sc.parallelize(srlists)
# print(rddsr.collect())
# consr=rddsr.map(lambda x : x +"krishna")
# print(consr.collect())
# repsr=rddsr.map(lambda x : x.replace("zeyo","gone"))
# print(repsr.collect())
# filsr=rddsr.filter(lambda x : 'zeyo' in x)
# print(filsr.collect())
#
# ls=["A~B","C~D"]
# rddls=sc.parallelize(ls)
# print("==raw rddls===")
# print(rddls.collect())
#
# flatrdd=rddls.flatMap(lambda x : x.split("~"))
# print("===flatrdd===")
# print(flatrdd.collect())


# data = [
#     "State->TN,City->Chennai",
#     "State->UP,City->Lucknow"
# ]

# rdds = sc.textFile("state.txt")
# print("===============RAW DATA=============")
# print(rdds.collect())
# print()
# flatdata = rdds.flatMap(lambda x : x.split(","))
# print("===============flatdata DATA=============")
# print(flatdata.collect())
# print()
# fildata=flatdata.filter(lambda x:'State' in x)
# print(fildata.collect())
# fildata.foreach(print)
# finalstate=fildata.map(lambda x : x.replace("State->"," "))
# finalstate.foreach(print)
#
# cityfil=flatdata.filter(lambda x:'City' in x)
# cityfil.foreach(print)
#
# finalcity=cityfil.map(lambda x: x.replace ("City->"," "))
# finalcity.foreach(print)
# data=sc.textFile("usdata.csv")
# data.foreach(print)
# fildata=data.filter(lambda x : len(x) > 200)
# fildata.foreach(print)
# flatdata=fildata.flatMap(lambda x : x.split(","))
# flatdata.foreach(print)
# repfdata=flatdata.map(lambda x: x.replace("-",""))
# repfdata.foreach(print)
# confdata=repfdata.map(lambda x:x + ",gopi")
# confdata.foreach(print)

# csvdff=spark.read.format("csv").option("header","true").load("usdata.csv")
# csvdff.show()
# fildf=csvdff.filter("state='LA'")
# fildf.show()
rdd1=sc.parallelize([("sai",10),("gopi",990),("ram",89),("gopi",66)],1)
rdd1.foreach(print)


