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

path='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.akas.tsv.gz'

schema1=s.schema_title_akas()


tabl1_df = rw.reading(spark_session, path, schema1)
tabl1_df.show()
tabl1_df.printSchema()

rez_task1_df=tabl1_df.filter(f.col('language') == 'uk')
rez_task1_df.show()

#rez_task1_df.write.csv('d:/IRINA/MyPython/imdb-spark-project-IT-1/Results/task1', header=True, mode="overwrite")


