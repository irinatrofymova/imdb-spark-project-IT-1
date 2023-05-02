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

path8_tb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema8_tb=s.schema_title_basics()
tabl8_tb_df = rw.reading(spark_session, path8_tb, schema8_tb)

path8_tr='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.ratings.tsv.gz'
schema8_tr=s.schema_title_ratings()
tabl8_tr_df = rw.reading(spark_session, path8_tr, schema8_tr)

tabl8_df=tabl8_tb_df.join(tabl8_tr_df, on='tconst').select('primaryTitle', 'genres', 'averageRating').filter(f.col('genres') != 'null')
#tabl8_df.show()

window = Window.orderBy(f.col('averageRating').desc()).partitionBy('genres')
rez_tabl=tabl8_df.withColumn('top', f.row_number().over(window))
#rez_tabl.show(70)

rez_tabl=rez_tabl.filter(f.col('top')<11)
rez_tabl.show(70)





