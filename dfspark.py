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
# data=sc.textFile("dt.txt")
# data.foreach(print)
# mapdata=data.map(lambda x: x.split(","))
# mapdata.foreach(print)
# from collections import namedtuple
# columns=namedtuple('columns',['id','tno','amt','category','product','mode'])
# ascol=mapdata.map(lambda x: columns(x[0],x[1],x[2],x[3],x[4],x[5]))
# prrfil=ascol.filter(lambda x : 'Gymnastics' in x.product)
# prrfil.foreach(print)
# df=prrfil.toDF()
# df.show()
# sedf=df.select("id","tno")
# sedf.show()
#read
# data = [
#     ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
#     ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
#     ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
#     ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
#     ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
#     ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
# df=spark.createDataFrame(data).toDF("tno","tdate","amt","category","product","spendby")
# df.show()
# selcoldf=df.select("tno","tdate")
# selcoldf.show()
# droppdf=df.drop("tno","tdate")
# droppdf.show()
# sinncolfil=df.filter("category='Exercise'")
# sinncolfil.show()
# mullcolfil=df.filter("category='Exercise' and spendby='cash'")
# mullcolfil.show()
# mullfilcolor=df.filter("category='Exercise'or spendby='cash'")
# mullfilcolor.show()
# multicol=df.filter("category in ('Exercise','Gymnastics')")
# multicol.show()
# productfil=df.filter("product like '%Gymnastics%'")
# productfil.show()
# pronull=df.filter("product is null")
# pronull.show()
# pronotnull=df.filter("product is not null")
# pronotnull.show()
# notfilcol=df.filter("category!='Exercise'")
# notfilcol.show()

#expressions
# seleexper=df.selectExpr(
#                       "cast(tno as int) as tno",
#                             "split(tdate,'-')[2] as year",
#                             "amt+50 as amount",
#                             "upper(category) as category",
#                             "concat(product,'~gopi') as product",
#                             "spendby",
#                             "case when spendby ='cash' then 1 when spendby='paytm' then 0 else 2 end as status"
#                       )
# seleexper.show()
#withcolumn##
# from pyspark.sql.functions import *
#
# withcolexp=df.withColumn("category",expr("upper(category)"))
# withcolexp.show()
# withnonexp=df.withColumn("status",expr("case when spendby='cash' then 0 when spendby='paytm' then 1 else 2 end "))
# withnonexp.show()
# data4 = [
#     (1, "raj"),
#     (2, "ravi"),
#     (3, "sai"),
#     (5, "rani")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "mouse"),
#     (3, "mobile"),
#     (7, "laptop")
# ]
#
# df2 = spark.createDataFrame(data3, ["id", "product"])
# df2.show()
# innerjoin=df1.join(df2,["id"],"inner")
# innerjoin.show()
# leftjoin=df1.join(df2,["id"],"left")
# leftjoin.show()
# rightjoin=df1.join(df2,["id"],"right")
# rightjoin.show()
# fulljoin=df1.join(df2,["id"],"full")
# fulljoin.show()
# leftanti=df1.join(df2,["id"],"left_anti")
# innerjoin.show()
# crosssjoin=df1.crossJoin(df2)
# crosssjoin.show()
# data4 = [
#     (1, "A"),
#     (2, "B"),
#     (3, "C"),
#     (4, "D")
# ]
#
# df1 = spark.createDataFrame(data4, ["id", "name"])
# df1.show()
#
# data3 = [
#     (1, "A"),
#     (2, "B"),
#     (4, "X"),
#     (5, "F")
# ]
#
# df2 = spark.createDataFrame(data3, ["id", "name1"])
# df2.show()
# fullf=df1.join(df2,["id"],"full")
# fullf.show()
# from pyspark.sql.functions import *
# withfull=fullf.withColumn("comment",expr("case when name=name1 then 'match' else 'mismatch' end"))
# withfull.show()
# filtt=withfull.filter("comment='mismatch'")
# filtt.show()
# withfil=filtt.withColumn("comment",expr("case when name1 is null then 'New in source' when name is null then 'New in target' else comment end"))
# withfil.show()
# droppfinal=withfil.drop("name","name1")
# droppfinal.show()

# data = [(1,"Veg Biryani"),(2,"Veg Fried Rice"),(3,"Kaju Fried Rice"),(4,"Chicken Biryani"),(5,"Chicken Dum Biryani"),(6,"Prawns Biryani"),(7,"Fish Birayani")]
#
# df1 = spark.createDataFrame(data,["food_id","food_item"])
# df1.show()
#
# ratings = [(1,5),(2,3),(3,4),(4,4),(5,5),(6,4),(7,4)]
#
# df2 = spark.createDataFrame(ratings,["food_id","rating"])
# df2.show()
# from pyspark.sql.functions import *
#
# # lefjoin = df1.join(df2, ["food_id"], "left").orderBy("food_id").withColumn("stats(out of 5)",expr("repeat('*',rating)"))
# # lefjoin.show()
# leffjoin=df1.join(df2,["food_id"],"left").orderBy("food_id").withColumn("stats(out of 5)",expr("repeat('*',rating)"))
# leffjoin.show()
data = [("A", "AA"), ("B", "BB"), ("C", "CC"), ("AA", "AAA"), ("BB", "BBB"), ("CC", "CCC")]

df = spark.createDataFrame(data, ["child", "parent"])
df.show()
df1=df
df2=df.withColumnRenamed("child","child1").withColumnRenamed("parent","parent1")
df1.show()
df2.show()

innerujoint=df1.join(df2,df1["child"]==df2["parent1"],"inner")
innerujoint.show()
dropujoint=innerujoint.drop("parent1")
dropujoint.show()
wirename=(
         dropujoint.withColumnRenamed("parent","parent1").withColumnRenamed("child","parent").withColumnRenamed("child1","child").withColumnRenamed("parent1","grandparent")

)
wirename.show()