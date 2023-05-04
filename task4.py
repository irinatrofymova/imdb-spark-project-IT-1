from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task4(spark_session):
    '''
    Gets names of people, corresponding movies/series and characters they played in those films.
    Defines dataframe schemas using functions from the Schemas module.
    Reads the files title.basics.tsv, name.basics.tsv and title.principals.tsv (a function from the read module) and forms dataframes.
    Prefixes in the names of dataframes, schemas, and file paths correspond to file names: _tb - title.basics,
    _nb - name.basics, _tp - title.principals.
    Links dataframes by key fields.
    Selects columns: primaryName, primaryTitle, and characters. Filters out missing values in the "characters" column.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path_tb='./data/title.basics.tsv.gz'
    schema4_tb=s.schema_title_basics()
    tabl4_tb_df=rw.reading(spark_session, path_tb, schema4_tb)
    path_nb='./data/name.basics.tsv.gz'
    schema4_nb=s.schema_name_basics()
    tabl4_nb_df=rw.reading(spark_session, path_nb, schema4_nb)
    path_tp='./data/title.principals.tsv.gz'
    schema4_tp=s.schema_title_principals()
    tabl4_tp_df=rw.reading(spark_session, path_tp, schema4_tp)
    tabl4_tp_df = tabl4_tp_df.withColumn('characters', f.when(f.col('characters').isin(r'\N', None), None).otherwise(f.col('characters')))
    task4_df=tabl4_tp_df.join(tabl4_nb_df, tabl4_tp_df.nconst==tabl4_nb_df.nconst, "inner") \
        .join(tabl4_tb_df, tabl4_tp_df.tconst==tabl4_tb_df.tconst, "inner") \
        .select(tabl4_nb_df.primaryName, tabl4_tb_df.primaryTitle, tabl4_tp_df.characters).filter(f.col('characters') != 'null')
    return task4_df