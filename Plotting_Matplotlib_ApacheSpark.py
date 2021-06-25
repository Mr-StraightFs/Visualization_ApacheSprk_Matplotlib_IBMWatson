from IPython.display import Markdown, display
import matplotlib.pyplot as plt

# Error Control
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    print('Kernel Error')


sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

# This function samples a dataset as follows , Our Variable of Intrest here is : Temperature
def getSample():
    return df.sample(False,0.1)

# This func prepare data for a hstogram plot
def getListForHistogramAndBoxPlot():
    return df.rdd.map(lambda row : row.temperature ).filter(lambda x:x is not None ).collect()

# This funct. prepares data for a runchart (time series plot)
def getListsForRunChart():
    result_rdd= df.rdd.map(lambda row,ts : row.temperature ).filter(lambda row:row[1] is not None ).sample(False,0.1)
    result_ts = result_rdd.map(lambda row:row[0]).collect()
    result_temp = result_rdd.map(lambda row:row[1]).collect()
    return (result_ts,result_temp)


# Now plotting
# hitogram
plt.hist(getListForHistogramAndBoxPlot())
plt.show()
#boxplot
plt.boxplot(getListForHistogramAndBoxPlot())
plt.show()
#runchart
lists = getListsForRunChart()
plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()



