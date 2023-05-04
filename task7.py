from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw

def task7(spark_session):
    '''
    Gets 10 titles of the most popular movies/series etc. by each decade.
    Defines dataframe schemas using functions from the Schemas module.
    Reads the files title.basics.tsv and title.ratings.tsv (a function from the read module) and forms dataframes.
    Prefixes in the names of dataframes, schemas, and file paths correspond to file names:
    _tb - title.basics, _tr - title.ratings.
    Links dataframes by key fields.
    Calculates the decade number (new column 'decade_number'):
    cuts out the first two characters (century) and the third character (decade) from the string "startYear"
    (for example, 1957: century - '19', decade - '5').
    Converts to IntegerType. Specifies the decade marker as century*10+decade (for example, 19*10+5=195).
    Filters out missing or null values in the 'decade_number' column.
    Applies Windows row_number function (new column 'top');
    partitioning by column 'decade_number', ordering by column 'averageRating', filter by column 'top' for values <11.
    Generates the decade number as a string (new column 'str_decade'):
    integer value *10 and converts to StringType; appends the string "-s"
    (for example, 195*10=1950; "1950" + "-s"; "1950-s").
    Selects columns: 'str_decade', 'primaryTitle', 'top'.
    :param spark_session: SparkSession
    :return: dataframe
    '''
    path7_tb='./data/title.basics.tsv.gz'
    schema7_tb=s.schema_title_basics()
    tabl7_tb_df = rw.reading(spark_session, path7_tb, schema7_tb)
    path7_tr='./data/title.ratings.tsv.gz'
    schema7_tr=s.schema_title_ratings()
    tabl7_tr_df = rw.reading(spark_session, path7_tr, schema7_tr)
    tabl7_df=tabl7_tb_df.join(tabl7_tr_df, on='tconst').select('primaryTitle', 'startYear', 'averageRating')

    tabl7_df=tabl7_df.withColumn('centure', f.col('startYear').substr(startPos=1, length=2))
    tabl7_df=tabl7_df.withColumn('dec', f.col('startYear').substr(startPos=3, length=1))
    tabl7_df=tabl7_df.withColumn('centure', tabl7_df['centure'].cast(t.IntegerType()))
    tabl7_df=tabl7_df.withColumn('dec', tabl7_df['dec'].cast(t.IntegerType()))
    tabl7_df=tabl7_df.withColumn('decade_number',f.col('centure')*10+f.col('dec'))
    tabl7_withDecade_df=tabl7_df.select('primaryTitle', 'startYear', 'averageRating', 'decade_number').filter(f.col('decade_number') > 0)

    window = Window.orderBy(f.col('averageRating').desc()).partitionBy('decade_number')
    rez_tabl=tabl7_withDecade_df.withColumn('top', f.row_number().over(window))
    rez_df=rez_tabl.filter(f.col('top')<11)

    rez_df=rez_df.withColumn('years_int',f.col('decade_number')*10)
    rez_df=rez_df.withColumn('years_str', rez_df['years_int'].cast(t.StringType()))
    rez_df=rez_df.withColumn('str_const', f.lit('-s'))
    rez_df=rez_df.withColumn('str_decade', f.concat(f.col('years_str'), f.col('str_const')))

    task7_df=rez_df.select('str_decade', 'primaryTitle', 'top')
    return task7_df