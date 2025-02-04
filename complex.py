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
# df=spark.read.format("json").option("multiline","true").load("file:///D:\cv.json")
# df.show()
# df.printSchema()
# data="""
#      {
# 	"place": "Hyderabad",
# 	"user": {
# 		"name": "zeyo",
# 		"address": {
# 			"number": "40",
# 			"street": "ashok nagar",
# 			"pin": "400209"
# 		}
# 	}
# }
# """
# rdds=sc.parallelize([data])
# df=spark.read.option("multiline","true").json(rdds)
# df.show()
# df.printSchema()
# flattendf = df.select(
#     "place",
#     "user.address.number",
#     "user.address.pin",
#     "user.address.street",
#     "user.name"
# )
#
# flattendf.show()
# flattendf.printSchema()
# data="""
#        {
#        "id":2,
#        "trainer":"sai",
#        "zeyoaddress": {
#             "permanentAddress": "hyderabad",
#             "temporaryAddress": "chennai"
#     }
#
# }
#
#  """
# rdds=sc.parallelize([data])
# df=spark.read.option("multiline","true").json(rdds)
# df.show()
# df.printSchema()
# from pyspark.sql.functions import *
# flattendf = (
#     df.withColumn("permanentAddress",expr("zeyoaddress.permanentAddress"))
#       .withColumn("temporaryAddress",expr("zeyoaddress.temporaryAddress"))
#       .drop("zeyoaddress")
# )
# flattendf.show()
# flattendf.printSchema()

# data="""
#        {
#        "id":2,
#        "trainer":"sai",
#        "zeyostudents": [
#                    "Arthi",
#                    "Gopi"
#        ]
#
# }
#
#  """
# rdds=sc.parallelize([data])
# df=spark.read.option("multiline","true").json(rdds)
# df.show()
# df.printSchema()
# flatendf=df.selectExpr(
#                       "id",
#                             "trainer",
#                             "explode(zeyostudents) as zeyostudents"
#
# )
# flatendf.show()
# flatendf.printSchema()
# data="""
#        {
#   "country" : "US",
#   "version" : "0.6",
#   "Actors": [
#     {
#       "name": "Tom Cruise",
#       "age": 56,
#       "BornAt": "Syracuse, NY",
#       "Birthdate": "July 3, 1962",
#       "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
#       "wife": null,
#       "weight": 67.5,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/73.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
#                 }
#     },
#     {
#       "name": "Robert Downey Jr.",
#       "age": 53,
#       "BornAt": "New York City, NY",
#       "Birthdate": "April 4, 1965",
#       "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
#       "wife": "Susan Downey",
#       "weight": 77.1,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/78.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
#                 }
#     }
#   ]
# }
#  """
# rdds=sc.parallelize([data])
# df=spark.read.option("multiline","true").json(rdds)
# df.show()
# df.printSchema()
# from pyspark.sql.functions import *
#
#
#
# actorsexplode=df.withColumn("Actors",expr("explode(Actors)"))
#
#
#
# actorsexplode.show()
#
# actorsexplode.printSchema()
#
# finalflatten = actorsexplode.select(
#
#
#
#     "Actors.Birthdate",
#
#     "Actors.BornAt",
#
#     "Actors.age",
#
#     "Actors.hasChildren",
#
#     "Actors.hasGreyHair",
#
#     "Actors.name",
#
#     "Actors.photo",
#
#     "Actors.picture.large",
#
#     "Actors.picture.medium",
#
#     "Actors.picture.thumbnail",
#
#     "Actors.weight",
#
#     "Actors.wife",
#
#     "country",
#
#     "version"
#
# )

# finalflatten.show()
#
#
#
# finalflatten.printSchema()


## URL DATA FULL CODE##

import os

import urllib.request

import ssl
urldata=(



    urllib.request

    .urlopen(

        "https://randomuser.me/api/0.8/?results=10",

        context=ssl._create_unverified_context()

    )

    .read()

    .decode('utf-8')



)
### PYSPARK
df = spark.read.json(sc.parallelize([urldata]))
df.show()
df.printSchema()
from pyspark.sql.functions import *
resultexp = df.withColumn("results",expr("explode(results)"))
resultexp.show()
resultexp.printSchema()
finalflatten =  resultexp.select(

    "nationality",

    "results.user.cell",

    "results.user.dob",

    "results.user.email",

    "results.user.gender",

    "results.user.location.city",

    "results.user.location.state",

    "results.user.location.street",

    "results.user.location.zip",

    "results.user.md5",

    "results.user.name.first",

    "results.user.name.last",

    "results.user.name.title",

    "results.user.password",

    "results.user.phone",

    "results.user.picture.large",

    "results.user.picture.medium",

    "results.user.picture.thumbnail",

    "results.user.registered",

    "results.user.salt",

    "results.user.sha1",

    "results.user.sha256",

    "results.user.username",

    "seed",

    "version"

)



finalflatten.show()



finalflatten.printSchema()


