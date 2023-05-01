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

path_tb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.basics.tsv.gz'
schema4_tb=s.schema_title_basics()
tabl4_tb_df=rw.reading(spark_session, path_tb, schema4_tb)

#tabl4_tb_df.show()

path_nb='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/name.basics.tsv.gz'
schema4_nb=s.schema_name_basics()
tabl4_nb_df=rw.reading(spark_session, path_nb, schema4_nb)

#tabl4_nb_df.show()

path_tp='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.principals.tsv.gz'
schema4_tp=s.schema_title_principals()
tabl4_tp_df=rw.reading(spark_session, path_tp, schema4_tp)

#tabl4_tp_df.show()

rez_task4_df=tabl4_tp_df.join(tabl4_nb_df, tabl4_tp_df.nconst==tabl4_nb_df.nconst, "inner") \
    .join(tabl4_tb_df, tabl4_tp_df.tconst==tabl4_tb_df.tconst, "inner") \
    .select(tabl4_nb_df.primaryName, tabl4_tb_df.primaryTitle, tabl4_tp_df.characters)
rez_task4_df.show(60, truncate=False)


