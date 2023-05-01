from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f




def reading(spark_session, path_to_file, schemaDF):
    from_file_df = spark_session.read.csv(path_to_file,
                                      sep='\t',
                                      header=True,
                                      nullValue='null',
                                      schema=schemaDF)
    return from_file_df