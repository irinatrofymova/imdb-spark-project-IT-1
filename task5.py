from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task5(spark_session):
    '''
    Gets information about how many adult movies/series etc. there are per region.
    Gets the top 100 of them from the region with the biggest count to the region with the smallest one.
    Defines dataframe schemas using functions from the Schemas module.
    Reads the files title.basics.tsv and title.akas.tsv (a function from the read module) and forms dataframes.
    Prefixes in the names of dataframes, schemas, and file paths correspond to file names:
    _tb - title.basics, _ta - title.akas.
    Links dataframes by key fields.
    Selects columns: 'primaryTitle', 'region', 'isAdult'.
    Applies a filter on the 'isAdult' column for values 1.
    Groups by the 'region' column, applies function count(), and sorts the 'count' column in descending order.
    Limits the list to 100 values.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path_ta='./data/title.akas.tsv.gz'
    schema_ta=s.schema_title_akas()
    tabl5_ta_df = rw.reading(spark_session, path_ta, schema_ta)
    tabl5_ta_df = tabl5_ta_df.withColumn('region', f.when(f.col('region').isin(r'\N', None), None).otherwise(f.col('region')))
    path_tb='./data/title.basics.tsv.gz'
    schema_tb=s.schema_title_basics()
    tabl5_tb_df = rw.reading(spark_session, path_tb, schema_tb)
    tabl5_df=tabl5_ta_df.join(tabl5_tb_df, tabl5_tb_df.tconst==tabl5_ta_df.titleID, "inner") \
        .select(tabl5_tb_df.primaryTitle,tabl5_ta_df.region, tabl5_tb_df.isAdult) \
        .filter((f.col('isAdult')==1) & (f.col('region') != 'null'))
    task5_df=tabl5_df.groupBy('region').count().orderBy('count', ascending=False).limit(100)
    return task5_df