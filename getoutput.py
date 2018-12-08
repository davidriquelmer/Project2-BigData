from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row
import csv

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionInstance' not in globals()):
        globals()['sparkSessionInstance'] = SparkSession.builder.config(conf=sparkConf) \
                                            .enableHiveSupport().getOrCreate()
    return globals()['sparkSessionInstance']

def output():
    spark = getSparkSessionInstance(sc.getConf())        
    df = spark.sql("use default")
    #df = spark.sql("select hashtag, sum(total) as suma from tb_hashtag group by hashtag order by suma desc limit 5")
    df = spark.sql("select * from tb_hashtag")
    df.show()
    df.repartition(1).write.csv("/data/filename.csv")

if __name__ == "__main__":
    sc = SparkContext(appName="Save CSV")
    output()
