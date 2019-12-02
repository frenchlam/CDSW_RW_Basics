# # Visualisation in CDSW
# Workbench is based on iPython and as such supports the most popular visualisation
# frameworks available for Python or R. 
# ### Known Limitation : 
#  - no support for ipywidgets
#  - some visualisation must be imported as IFrames ( ex some plotly graphs ) 
#  - single line evaluation
 

# ## **0. Load data**
# We'll be using Spark to access data for 2 reasons : 
# - Integration with the CDH and HDP platforms
# - Distributed computing : 
#   When working with large dataset, it is often impossible to visualise the entire
#   dataset directly. 
#   Pre-processing must be done to reduce dimensionality to a size "acceptable" for
#   most visualisation libraries 
#   -  Agregation 
#   -  Sampling

# ### Start Spark session
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
  .master('yarn') \
  .config("spark.executor.instances","3")\
  .config("spark.executor.memory","2g")\
  .appName('Visualisation') \
  .getOrCreate()


# ### Acess data from Hive ( prepared by the setup.sh script )
# The table contains a fairly "large" dataset ( 5.2 M lines ).
# Based on ASA airline on-time dataset [http://stat-computing.org/dataexpo/2009/]
# - using Year 1988

spark.sql('''describe table flights.flights_raw''').show(50)

# ### Simple data quality analysis
flight_raw_df = spark.sql('select * from flights.flights_raw')
flight_raw_df.cache()

# #### Number of rows 
print("\nDataset has : {} rows".format(flight_raw_df.count()))

# #### Number of null values for each columns 
for col in flight_raw_df.columns: 
  count = flight_raw_df.filter(flight_raw_df[col].isNull()).count()
  print('{} has {} nulls'.format(col,count))



# ### **1. Visual analysis 
# Most visualisation will fail for large volumes > ~500k/1M
# Ex : Trying to bring the data back as a Pandas Dataframe will crash the driver

# pandas_df = flight_raw_df.toPandas()

# ### Approach 1 - Sampling 
# #### Question 1 : Departure delay distribution
# using seaborn

pandas_df_Dep_delay = flight_raw_df.select(['DepDelay']).filter(flight_raw_df['DepDelay'].isNotNull())\
  .sample(False, 0.1 , seed=30)\
  .toPandas()
pandas_df_Dep_delay.info()


%matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns

# ##### Limitation : single line for plots 
sns.distplot(pandas_df_Dep_delay['DepDelay'], bins=300, kde=False)\
  .set(xlim=(-10,200),
       ylim=(0,None),
       title="Basic DistPlot")
  
  
# ##### Solution : encapsulate plot in a function 

# remove long tail and display
pandas_df_Dep_delay2 = flight_raw_df.select(['DepDelay'])\
  .filter(flight_raw_df['DepDelay'].isNotNull() & (flight_raw_df['DepDelay'] < 20) )\
  .sample(False, 0.1 , seed=500)\
  .toPandas()
pandas_df_Dep_delay2.info()

def histplot(a,b) : 
  plt.hist([a,b], bins= 20, color=['r','b'], range=[-10,60])
  plt.title ('histograms')
  plt.xlabel('Flight Delay')
      
histplot(pandas_df_Dep_delay['DepDelay'],pandas_df_Dep_delay2['DepDelay'])



### Approach2 agregation and data pruning
  
# ### Data Selection 
# Pre Selecting columns of interest (leaving out columns with nulls)

spark_data_df = spark.sql(
  '''select month, DayofMonth,DayOfWeek,CRSDepTime,CRSArrTime,UniqueCarrier,
            UniqueCarrier,FlightNum,CRSElapsedTime,Origin,Dest,Cancelled,Diverted
      from flights.flights_raw
  ''')            
spark_data_df.cache()
spark_data_df.createOrReplaceTempView('flights')

# Free up mem ressources from old dataframe
flight_raw_df.unpersist()




# #### Question 2 : Which airlines have, proportionally the most cancelations (top 20)
pandas_df = spark.sql(
  '''Select c.Description as airline, c.code, f.avg_cancel, nb_flights, nb_cancelled
     FROM (
           SELECT UniqueCarrier, (sum(cancelled)/sum(1))*100 as avg_cancel, 
                  sum(1) as nb_flights, sum(cancelled) as nb_cancelled
           FROM flights
           GROUP By UniqueCarrier
           ORDER by avg_cancel DESC
           ) f 
      INNER JOIN flights.carriers c ON c.code = f.UniqueCarrier
  ''').toPandas()
pandas_df.head(20)


import matplotlib.pyplot as plt
import seaborn as sns

sns.barplot(x='airline', y='avg_cancel',
           data=pandas_df,
           color='blue',
           saturation=.5)


# #### using plotly
# #### Limited support 
# Cannot display directly in Workbench.
# 1. Save to HTML 
# 2. Display IFrame

import chart_studio.plotly as py
import plotly.graph_objects as go
from IPython.core.display import display, HTML

fig = go.Figure(data=
                go.Bar(x=pandas_df.airline,
                       y=pandas_df.avg_cancel)
               )
fig.write_html('/cdn/plotly_figure.html', auto_open=True)
HTML("<iframe height='400' width='800' src=plotly_figure.html>")


