from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe
import pyspark.sql.types as t
import pyspark.sql.functions as f
import schemas as s
import read_write as rw
import task1 as t1
import task2 as t2
import task3 as t3
import task4 as t4
import task5 as t5
import task6 as t6
import task7 as t7
import task8 as t8

def main():
    '''
    Sequentially calls functions for 8 tasks from the corresponding files (task1-task8).
    Writes results to files
    :return:
    '''
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    # Executing task 1 and writing the results to the file
    task1_df=t1.task1(spark_session)
    path_to_write_1='./Results/task1'
    rw.writing(spark_session, path_to_write_1, task1_df)
    # Executing task 2 and writing the results to the file
    task2_df=t2.task2(spark_session)
    path_to_write_2 = './Results/task2'
    rw.writing(spark_session, path_to_write_2, task2_df)
    # Executing task 3 and writing the results to the file
    task3_df=t3.task3(spark_session)
    path_to_write_3 = './Results/task3'
    rw.writing(spark_session, path_to_write_3, task3_df)
    # Executing task 4 and writing the results to the file
    task4_df=t4.task4(spark_session)
    path_to_write_4 = './Results/task4'
    rw.writing(spark_session, path_to_write_4, task4_df)
    # Executing task 5 and writing the results to the file
    task5_df=t5.task5(spark_session)
    path_to_write_5 = './Results/task5'
    rw.writing(spark_session, path_to_write_5, task5_df)
    # Executing task 6 and writing the results to the file
    task6_df=t6.task6(spark_session)
    path_to_write_6 = './Results/task6'
    rw.writing(spark_session, path_to_write_6, task6_df)
    # Executing task 7 and writing the results to the file
    task7_df=t7.task7(spark_session)
    path_to_write_7 = './Results/task7'
    rw.writing(spark_session, path_to_write_7, task7_df)
    # Executing task 8 and writing the results to the file
    task8_df=t8.task8(spark_session)
    path_to_write_8 = './Results/task8'
    rw.writing(spark_session, path_to_write_8, task8_df)

    return

if __name__ == "__main__":
    main()