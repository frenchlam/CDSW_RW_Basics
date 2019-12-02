# # Visualisation in CDSW
# Workbench is based on iPython and as such supports the most popular visualisation
# frameworks available for Python or R. 
# ### Known Limitation : 
#  - no support for ipywidgets
#  - some visualisation must be imported as IFrames ( ex some plotly graphs ) 
 

# ## 0. Load data
# We'll be using Spark to access data for 2 reasons : 
# - Integration with 
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
  .config("spark.executor.instances","2")\
  .config("spark.executor.memory","2g")\
  .appName('Visualisation') \
  .getOrCreate()


# ### Acess data from Hive ( prepared by the setup.sh script )
# The table contains a fairly "large" dataset ( 5.2 M lines ).
# Based on ASA airline on-time dataset [http://stat-computing.org/dataexpo/2009/]
# - using Year 1988

spark.sql('''describe table flights.flights_raw''').show(50)

# ### Simple data quality analysis
# Count number of null values for each columns 
flight_raw_df = spark.sql('select * from flights.flights_raw')
flight_raw_df.cache()

from timeit import default_timer as timer

start = timer()

for col in flight_raw_df.columns: 
  count = flight_raw_df.filter(flight_raw_df[col].isNull()).count()
  print('{} has {} nulls'.format(col,count))
end = timer()
print ( end - start )


for col in spark.sql('select * from flights.flights_raw').columns : 
  count = 
  print(col)



# ### Data Selection 
# Pre Selecting columns of interest
spark_data_df = spark.sql('''select month, Cancelled
            from flights.flights_raw''').cache()
spark_data_df.createOrReplaceTempView('canceled_month')

spark_data_df.count()


sample_data = spark.sql('''select month, Cancelled
            from flights.flights_raw''')\
            .sample(False, 0.02 , seed=52)\
            .toPandas()
    
data.info()


get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import seaborn as sb

sb.distplot(data['month'], kde=False)

## Bar plot

bar_plot_data =spark.sql(
  '''select month, count(Cancelled) as nb_cancelled
  from canceled_month
  group by month 
  order by month''').toPandas()

sb.barplot(x='month', y='nb_cancelled',
           data=bar_plot_data,
           color='blue',
           saturation=.5)