from pyspark.sql import SparkSession
from pyspark.sql.types import *

# ## Start spark session
spark = SparkSession.builder \
  .master('yarn') \
  .config("spark.executor.instances","2")\
  .config("spark.executor.memory","2g")\
  .appName('Visualisation') \
  .getOrCreate()


#spark.sql('CREATE TABLE flights.flights LIKE flights.flights_raw')
#spark.sql('''ALTER TABLE flights.flights ADD IF NOT EXISTS PARTITION month int''')
#statement='''INSERT OVERWRITE TABLE flights.flights PARTITION(Month) 
#SELECT * FROM flights.flights_raw'''

spark.sql('''describe table flights.flights_raw''').show(50)


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

sb.barplot(x='month', y='nb_cancelled',data=bar_plot_data)