from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task3(spark_session):
    '''
    Gets titles of all movies that last more than 2 hours (120 seconds).
    Defines the schema of a dataframe using a function from the module schemas.py.
    Reads the title_basics.tsv.gz file (a function from the module read_write.py) and forms a dataframe.
    Applies a filter on the 'titleType' column for values 'movie' and on the 'runtimeMinutes' column for values '>120'.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path='./data/title.basics.tsv.gz'
    schema3=s.schema_title_basics()
    tabl3_df = rw.reading(spark_session, path, schema3)
    task3_df=tabl3_df.select('primaryTitle', 'titleType', 'runtimeMinutes').filter((f.col('runtimeMinutes') > 120) & (f.col('titleType')=='movie'))
    return task3_df