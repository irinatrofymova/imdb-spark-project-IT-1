from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task1(spark_session):
    '''
    Gets all titles of series/movies etc. that are available in Ukrainian.
    Defines the schema of a dataframe using a function from the module schemas.py.
    Reads the title_akas file (a function from the module read_write.py) and forms a dataframe.
    Applies a filter on the 'language' column for 'uk' values
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path='./data/title.akas.tsv.gz'
    schema1=s.schema_title_akas()
    tabl1_df = rw.reading(spark_session, path, schema1)
    tabl1_df = tabl1_df.withColumn('language', f.when(f.col('language').isin(r'\N', None), None).otherwise(f.col('language')))
    task1_df=tabl1_df.select('title', 'language').filter(f.col('language') == 'uk')
    return task1_df

