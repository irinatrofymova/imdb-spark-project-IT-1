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

path_ta='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.akas.tsv.gz'
schema_ta=s.schema_title_akas()
tabl5_ta_df = rw.reading(spark_session, path_ta, schema_ta)
tabl5_ta_df = tabl5_ta_df.withColumn('region', f.when(f.col('region').isin(r'\N', None), None).otherwise(f.col('region')))

#tabl5_ta_df.show()

path_tb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema_tb=s.schema_title_basics()
tabl5_tb_df = rw.reading(spark_session, path_tb, schema_tb)
#tabl5_tb_df.show()

tabl5_df=tabl5_ta_df.join(tabl5_tb_df, tabl5_tb_df.tconst==tabl5_ta_df.titleID, "inner") \
    .select(tabl5_tb_df.primaryTitle,tabl5_ta_df.region, tabl5_tb_df.isAdult) \
    .filter((f.col('isAdult')==1) & (f.col('region') != 'null'))
#tabl5_df.show()

rez5=tabl5_df.groupBy('region').count().orderBy('count', ascending=False)
rez5.show()




