import dlt
from pyspark import pipelines as dp

@dp.temporary_view()
def bronze_chicago():
    df = spark.read\
            .format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
            .option("delimiter", "\t")\
            .option("multiLine", "true")\
            .load("/Volumes/workspace/damg7370/restaurant_data/chicago")
    return df

@dp.temporary_view()
def bronze_dallas():
    df = spark.read\
            .format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
            .option("delimiter", "\t")\
            .option("multiLine", "true")\
            .load("/Volumes/workspace/damg7370/restaurant_data/dallas")
    return df