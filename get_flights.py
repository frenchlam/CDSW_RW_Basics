# ## Get data using Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder \
  .master('yarn') \
  .config("spark.executor.instances","2")\
  .config("spark.executor.memory","2g")\
  .appName('Visualisation') \
  .getOrCreate()
  
flights_df = spark.sql('''
  select distinct UniqueCarrier, FlightNum, origin, dest
  from flights.flights_raw
  ''').toPandas()

flights_df.info()

# #### release spark ressources
spark.stop()


# ## airport name lookup function (calling other model)

def lookup_airport(code: str):
  import requests
  r = requests.post('http://mlamairesse-training-4.vpc.cloudera.com/api/altus-ds-1/models/call-model', \
                    data='{"accessKey":"m83vkr2ccx32v41psltktrwyhy2czzi9","request":{"code":"%s"}}'%(code), \
                    headers={'Content-Type': 'application/json'})

  return r.json().get('response')


# ## Flight Lookup function 

def flight_lookup(args: dict):
  carrier = args['carrier'].upper()
  flight  = int(args['flight_num'])

  #filter dataframe
  flight_lookup = flights_df[(flights_df.FlightNum==flight) & (flights_df.UniqueCarrier==carrier) ]

  # lookup names from model
  if 'lookup_name' in args : 
    if args['lookup_name'].upper()=="TRUE": 
      
      airport_name_lst = []
      for ori in flight_lookup['origin']:
        airport_name_lst.append(lookup_airport(ori).get('airports.airport'))

      # add column to pandas df
      flight_lookup['airport_name'] = airport_name_lst 
  
  return flight_lookup.to_json(orient='records')


#flight_lookup({"carrier":"DL", "flight_num":"261", "lookup_name":"true"})