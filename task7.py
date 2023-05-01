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

path7_tb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema7_tb=s.schema_title_basics()

tabl7_tb_df = rw.reading(spark_session, path7_tb, schema7_tb)
#tabl7_tb_df.show()

path7_tr='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.ratings.tsv.gz'
schema7_tr=s.schema_title_ratings()

tabl7_tr_df = rw.reading(spark_session, path7_tr, schema7_tr)
#tabl7_tr_df.show()

tabl7_df=tabl7_tb_df.join(tabl7_tr_df, on='tconst').select('primaryTitle', 'startYear', 'averageRating')
#tabl7_df.show()
tabl7_df=tabl7_df.withColumn('centure', f.col('startYear').substr(startPos=1, length=2))
tabl7_df=tabl7_df.withColumn('dec', f.col('startYear').substr(startPos=3, length=1))
tabl7_df=tabl7_df.withColumn('centure', tabl7_df['centure'].cast(t.IntegerType()))
tabl7_df=tabl7_df.withColumn('dec', tabl7_df['dec'].cast(t.IntegerType()))
tabl7_df=tabl7_df.withColumn('decade_number',f.col('centure')*10+f.col('dec'))

#tabl7_df.show()
tabl7_withDecade_df=tabl7_df.select('primaryTitle', 'startYear', 'averageRating', 'decade_number').filter(f.col('decade_number') > 0)
#tabl7_withDecade_df.show()

window = Window.orderBy('averageRating').partitionBy('decade_number', 'averageRating')
rez_tabl=tabl7_withDecade_df.withColumn('new', f.count(f.col('decade_number')).over(window))
rez_tabl.show()
