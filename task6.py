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

path_te='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.episode.tsv.gz'
schema6_te=s.schema_title_episode()
tabl6_te_df=rw.reading(spark_session, path_te, schema6_te)

path_tb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema6_tb=s.schema_title_basics()
tabl6_tb_df = rw.reading(spark_session, path_tb, schema6_tb)

series_df=tabl6_tb_df.join(tabl6_te_df, tabl6_tb_df.tconst==tabl6_te_df.parentTconst, "inner") \
    .select(tabl6_tb_df.primaryTitle, tabl6_te_df.episodeNumber) \
    .filter(f.col('episodeNumber')>0)
#series_df.show()

rez6_df = series_df.groupBy('primaryTitle').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False)
rez6_df.show(50)

