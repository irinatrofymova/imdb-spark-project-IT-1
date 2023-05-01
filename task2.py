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

path='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/name.basics.tsv.gz'
schema2=s.schema_name_basics()


tabl2_df = rw.reading(spark_session, path, schema2)
tabl2_df.show()
#tabl2_df.printSchema()

rez_task2_df=tabl2_df.select('primaryName', 'birthYear').filter(f.col('birthYear') < 1901)
rez_task2_df.show()