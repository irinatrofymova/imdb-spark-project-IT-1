from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task8(spark_session):
    '''
    Gets 10 titles of the most popular movies/series etc. by each genre.
    Defines dataframe schemas using functions from the Schemas module.
    Reads the files title.basics.tsv and title.ratings.tsv (a function from the read module) and forms dataframes.
    Prefixes in the names of dataframes, schemas, and file paths correspond to file names:
    _tb - title.basics, _tr - title.ratings.
    Links dataframes by key fields.
    Selects the first genre listed (new column 'first_genre').
    Applies Windows row_number function (new column 'top');
    partitioning by column 'first_genre', ordering by column 'averageRating', filter by column 'top' for values <11.
    Selects columns: 'first_genre', 'primaryTitle', 'top'.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path8_tb='./data/title.basics.tsv.gz'
    schema8_tb=s.schema_title_basics()
    tabl8_tb_df = rw.reading(spark_session, path8_tb, schema8_tb)
    path8_tr='./data/title.ratings.tsv.gz'
    schema8_tr=s.schema_title_ratings()
    tabl8_tr_df = rw.reading(spark_session, path8_tr, schema8_tr)
    tabl8_df=tabl8_tb_df.join(tabl8_tr_df, on='tconst').select('primaryTitle', 'genres', 'averageRating').filter(f.col('genres') != 'null')

    tabl8_df=tabl8_df.withColumn('first_genre', f.substring_index(f.col('genres'), ',', 1))

    window = Window.orderBy(f.col('averageRating').desc()).partitionBy('first_genre')
    rez_tabl=tabl8_df.withColumn('top', f.row_number().over(window))

    task8_df=rez_tabl.select('first_genre', 'primaryTitle', 'top').filter(f.col('top')<11)
    return task8_df