from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task2(spark_session):
    '''
    Gets the list of people’s names, who were born in the 19th century.
    Defines the schema of a dataframe using a function from the module schemas.py.
    Reads the name_basics.tsv.gz file (a function from the module read_write.py) and forms a dataframe.
    Applies a filter on the 'birthYear' column for values '<1901'.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path='./data/name.basics.tsv.gz'
    schema2=s.schema_name_basics()
    tabl2_df = rw.reading(spark_session, path, schema2)
    task2_df=tabl2_df.select('primaryName', 'birthYear').filter(f.col('birthYear') < 1901)
    return task2_df