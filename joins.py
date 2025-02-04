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
#joins
#inner join

data4 = [
    (1, "raj"),
    (2, "ravi"),
    (3, "sai"),
    (5, "rani")
]

df1 = spark.createDataFrame(data4, ["id", "name"])
df1.show()

data3 = [
    (1, "mouse"),
    (3, "mobile"),
    (7, "laptop")
]

df2 = spark.createDataFrame(data3, ["id1", "product"])
df2.show()
# print("==inner join==")
# innerjoin=df1.join(df2,["id"],"inner").orderBy("id")
# innerjoin.show()
# print("==left join==")
# leftjoin=df1.join(df2,["id"],"left").orderBy("id")
# leftjoin.show()
#
# print("==right join==")
# rightjoin=df1.join(df2,["id"],"right").orderBy("id")
# rightjoin.show()
# print("==full join==")
# fulljoin=df1.join(df2,["id"],"full").orderBy("id")
# fulljoin.show()
# print("==left_anti join==")
# leftantijoin=df1.join(df2,["id"],"left_anti").orderBy("id")
# leftantijoin.show()
# print("==cross join==")
# cros=df1.crossJoin(df2)
# cros.show()


#inner
innerj=df1.join(df2,df1["id"]==df2["id1"],"inner").drop("id1").orderBy("id")
innerj.show()
leftj=df1.join(df2,df1["id"]==df2["id1"],"left").drop("id1").orderBy("id")
leftj.show()
rightj=df1.join(df2,df1["id"]==df2["id1"],"right").drop("id").orderBy("id1")
rightj.show()
from pyspark.sql.functions import *
fullj=df1.join(df2,df1["id"]==df2["id1"],"full").withColumn("id",expr("case when id is null then id1 else id end")).drop("id1").orderBy("id")
fullj.show()



