from IPython.display import Markdown, display


#Error Control
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('Import Error ')

# start a spark session
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()

# Defining the Functions for Mean Min , Max , Std , Corr , Skewness and Kurtosis , al functions take a dataframe Object , and the index of the column on whch to do the calculations
def minDF(df,x):
    return spark.sql("SELECT min(x) as minx from df").first().minx

def meanDF(df,x):
    return spark.sql("SELECT mean(x) as meanx from df").first().meanx

def maxDF(df,x):
    return spark.sql("SELECT max(x) as maxx from df").first().maxx

def sdTDF(df,x):
    return spark.sql("SELECT std(x) as stdx from df").first().stdtx

def skewDF(df,x):
    return spark.sql("""
    SELECT 
        (
            1/count(x)
        ) *
        SUM (
            POWER(x-%s,3)/POWER(%s,3)
        )

    as skwx from df
                        """ % (meanDF(), sdTDF())).first().skwx

def kurtosisDF(df,x):
    return spark.sql("""
SELECT 
    (
        1/count(temperature)
    ) *
    SUM (
        POWER(x-%s,4)/POWER(%s,4)
    )
as ktx from washing
                    """ % (meanTemperature(), sdTemperature())).first().ktx

def correlationTemperatureHardness(df,x,y):
    return df.stat.corr('x','y')




