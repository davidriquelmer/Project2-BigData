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
    df = spark.sql("select kword, sum(total) as suma from ejercicio4b  where times between cast('2018-12-07 12:00:00' as timestamp)- INTERVAL 1 HOUR and cast('2018-12-07 12:00:00' as timestamp) group by kword  order by suma desc limit 10")
    df.show()
    df.repartition(1).write.csv("/data/fndavide2.csv")

if __name__ == "__main__":
    sc = SparkContext(appName="Ejercicio 4.b")
    output()
