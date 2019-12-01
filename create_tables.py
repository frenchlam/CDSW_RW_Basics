from pyspark.sql import SparkSession
from pyspark.sql.types import *


print("start session")

print("Start Spark session :")
spark = SparkSession.builder \
  .master('yarn') \
  .config("spark.executor.instances","2")\
  .config("spark.executor.memory","2g")\
  .appName('wine-quality-create-table') \
  .getOrCreate()

print("save airlines files")   


# ### Create flights database
spark.sql('''drop database flights CASCADE''')
spark.sql('''create database if not exists flights''')

    
 
# ### save Flights table 
# read table
path='airlines/flights/' 

flights_df = spark.read.csv(
    path=path,
    header=True,
    sep=',',
    inferSchema=True,
    nullValue=None
).cache()
flights_df.printSchema()

# save in Hive
flights_df.orderBy(['Month','DayofMonth']).coalesce(4)\
    .write.format('orc').mode("overwrite")\
    .saveAsTable('flights.flights_raw')

print("Flights table saved")  
    
# ### save airports table 
# read table
path='airlines/airports' #HDFS location

airports_df = spark.read.csv(
    path=path,
    header=True,
    sep=',',
    inferSchema=True,
    nullValue=None
).cache()
airports_df.printSchema()

# save in Hive
airports_df.orderBy(['state','airport']).coalesce(2)\
    .write.format('parquet').mode("overwrite")\
    .saveAsTable('flights.airports')
    
print("airports table saved")  
   

# ### save carriers table 
# read table
path='airlines/carriers' #HDFS location

carriers_df = spark.read.csv(
    path=path,
    header=True,
    sep=',',
    inferSchema=True,
    nullValue=None
).cache()
carriers_df.printSchema()

# save in Hive
carriers_df.orderBy(['Code']).coalesce(2)\
    .write.format('parquet').mode("overwrite")\
    .saveAsTable('flights.carriers')
    
print("carriers table saved")  
  
spark.stop()