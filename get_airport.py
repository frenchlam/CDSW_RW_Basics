# ## load data (from Hive)
from pyhive import hive
import pandas as pd

conn=hive.Connection(host='mlamairesse-training-1.vpc.cloudera.com', port=10000, auth='KERBEROS', 
                     kerberos_service_name='hive')
airlines_pd_df = pd.read_sql('select * from flights.airports',conn)
airlines_pd_df.set_index('airports.iata',inplace=True)


# ##lookup function 

def lookup(arg: dict) : 
  code = arg['code'].upper()
  return airlines_pd_df.loc[code,:].to_dict()

lookup({"code":"WRL"})