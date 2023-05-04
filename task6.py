from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task6(spark_session):
    '''
    Gets information about how many episodes in each TV Series.
    Gets the top 50 of them starting from the TV Series with the biggest quantity of episodes.
    Defines dataframe schemas using functions from the Schemas module.
    Reads the files title.basics.tsv and title.episode.tsv (a function from the read module) and forms dataframes.
    Prefixes in the names of dataframes, schemas, and file paths correspond to file names:
    _tb - title.basics, _te - title.episode.
    Links dataframes by key fields.
    Selects columns: 'primaryTitle', 'episodeNumber'. Filters out null values in the 'episodeNumber' column.
    Groups by the 'primaryTitle', applies count function to the 'episodeNumber' column, and sorts the 'count' in descending order.
    Limits the list to 50 values.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path_te='./data/title.episode.tsv.gz'
    schema6_te=s.schema_title_episode()
    tabl6_te_df=rw.reading(spark_session, path_te, schema6_te)
    path_tb='./data/title.basics.tsv.gz'
    schema6_tb=s.schema_title_basics()
    tabl6_tb_df = rw.reading(spark_session, path_tb, schema6_tb)
    series_df=tabl6_tb_df.join(tabl6_te_df, tabl6_tb_df.tconst==tabl6_te_df.parentTconst, "inner") \
        .select(tabl6_tb_df.primaryTitle, tabl6_te_df.episodeNumber) \
        .filter(f.col('episodeNumber')>0)
    task6_df = series_df.groupBy('primaryTitle').agg({'episodeNumber': 'count'}).orderBy('count(episodeNumber)', ascending=False).limit(50)
    return task6_df
