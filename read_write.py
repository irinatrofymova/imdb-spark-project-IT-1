from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f


def reading(spark_session, path_to_file, schemaDF):
    '''
    Reads data from a file.
    Forms a dataframe according to the schema.
    :param spark_session: SparkSession
    :param path_to_file: Path to the file from which information is read.
    :param schemaDF: Dataframe schema
    :return: dataframe
    '''
    from_file_df = spark_session.read.csv(path_to_file,
                                      sep=r'\t',
                                      header=True,
                                      nullValue='null',
                                      schema=schemaDF)
    return from_file_df

#def writing(spark_session, path_to_file, to_file_df):
#    header=True
#    mode="owerwrite"
#    to_file_df.write.csv(path_to_file, header, mode)
#    return
