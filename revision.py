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
# maplit=data.map(lambda x :x.split(","))
# print("====mappsplit===")
# maplit.foreach(print)
# from collections import namedtuple
# columns=namedtuple('columns',['tno','tdate','amt','category','product','spendby'])
# assi=maplit.map(lambda x :columns(x[0],x[1],x[2],x[3],x[4],x[5]))
# prfilter=assi.filter(lambda x : 'Gymnastics' in x.product)
# prfilter.foreach(print)
# df=prfilter.toDF()
# df.show()
# # df.createOrReplaceTempView("BGT")
# # spark.sql("select tno,tdate from BGT").show()
# seledf=df.select("tno","tdate")
# seledf.show()

data = [
    ("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
    ("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
    ("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
    ("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
    ("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
    ("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
]
df=spark.createDataFrame(data).toDF("tno","tdate","amt","category","product","spendby")
df.show()
print("===dsl filters===")
sincol=df.filter("category='Exercise'")
sincol.show()

mulcol=df.filter("category='Exercise' and spendby='cash'")#and
mulcol.show()
mulcol=df.filter("category='Exercise' or spendby='cash'")#or
mulcol.show()
#same column multifilter
mulcol=df.filter("category in ('Exercise','Gymnastics')")
mulcol.show()

prodfil=df.filter("product like '%Gymnastics%'")
prodfil.show()

nullfil=df.filter("product is null")
nullfil.show()
notnullfil=df.filter("product is not null")
notnullfil.show()
notfilter=df.filter("category!='Exercise'")
notfilter.show()
print("===Expressions=================")
from pyspark.sql.functions import *
profil=df.selectExpr("cast(tno as int) as tno",
                           "split(tdate,'-')[2] as tdate",  #selection----processing
                           "amt + 100 as amt",
                           "upper(category) as category",  #processing
                           "concat(product,'~Gopi') as product",
                           "spendby",
                           "case when spendby='cash' then 0 when spendby='paytm' then 2 else 1 end as status "
                 )
profil.show()

