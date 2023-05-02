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

path7_tr='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.ratings.tsv.gz'
schema7_tr=s.schema_title_ratings()
tabl7_tr_df = rw.reading(spark_session, path7_tr, schema7_tr)

tabl7_df=tabl7_tb_df.join(tabl7_tr_df, on='tconst').select('primaryTitle', 'startYear', 'averageRating')
#tabl7_df.show()
tabl7_df=tabl7_df.withColumn('centure', f.col('startYear').substr(startPos=1, length=2))
tabl7_df=tabl7_df.withColumn('dec', f.col('startYear').substr(startPos=3, length=1))
tabl7_df=tabl7_df.withColumn('centure', tabl7_df['centure'].cast(t.IntegerType()))
tabl7_df=tabl7_df.withColumn('dec', tabl7_df['dec'].cast(t.IntegerType()))
tabl7_df=tabl7_df.withColumn('decade_number',f.col('centure')*10+f.col('dec'))

tabl7_withDecade_df=tabl7_df.select('primaryTitle', 'startYear', 'averageRating', 'decade_number').filter(f.col('decade_number') > 0)

window = Window.orderBy(f.col('averageRating').desc()).partitionBy('decade_number')
rez_tabl=tabl7_withDecade_df.withColumn('top', f.row_number().over(window))
#rez_tabl.show(70)

rez_df=rez_tabl.filter(f.col('top')<11)
#rez_df.show(70)
rez_df=rez_df.withColumn('years_int',f.col('decade_number')*10)
rez_df=rez_df.withColumn('years_str', rez_df['years_int'].cast(t.StringType()))
rez_df=rez_df.withColumn('str_const', f.lit('-s'))
rez_df=rez_df.withColumn('str_decade', f.concat(f.col('years_str'), f.col('str_const')))
#rez_df.show()
answer_table=rez_df.select('str_decade', 'primaryTitle', 'top')
answer_table.show()
