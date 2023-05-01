from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe

def main():
    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())

   # col=["lang", "users"]
   # dat=[("java", 2500), ("Scala", 12000), ("Py", 2000)]
   # movies_df=spark_session.read.csv(title.ratings.tsv)
   # movies_df.show()
   # test_df=spark_session.createDataFrame(dat).toDF(*col)
   # test_df.show()
   # test_df.printSchema()

if __name__ == "__main__":
    main()