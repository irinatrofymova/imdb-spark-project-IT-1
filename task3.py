from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

path='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema3=s.schema_title_basics()

tabl3_df = rw.reading(spark_session, path, schema3)

rez_task3_df=tabl3_df.select('primaryTitle', 'runtimeMinutes').filter(f.col('runtimeMinutes') > 120)
rez_task3_df.show(truncate=False)