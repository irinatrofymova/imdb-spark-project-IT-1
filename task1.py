from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task1(spark_session):
    path='d:/IRINA/MyPython/imdb-spark-project-IT-1/data/title.akas.tsv.gz'
    schema1=s.schema_title_akas()
    tabl1_df = rw.reading(spark_session, path, schema1)

    tabl1_df = tabl1_df.withColumn('language', f.when(f.col('language').isin(r'\N', None), None).otherwise(f.col('language')))
    task1_df=tabl1_df.select('title', 'language').filter(f.col('language') == 'uk')

    return task1_df

