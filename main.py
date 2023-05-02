from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw
import task1 as t1


def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    task1_df=t1.task1(spark_session)
    task1_df.show()
    #path_to_write='d:/IRINA/MyPython/imdb-spark-project-IT-1/Results/task1'
    #rw.writing(spark_session, path_to_write, task1_df)




if __name__ == "__main__":
    main()