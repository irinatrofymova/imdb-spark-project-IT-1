'''
The file contains functions for generating dataframe schemas.
'''
from pyspark import SparkConf
from pyspark.sql import SparkSession, dataframe, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f


def schema_title_akas():

    schema=t.StructType([t.StructField('titleID', t.StringType(), True),
                     t.StructField('ordering', t.IntegerType(), True),
                     t.StructField('title', t.StringType(), True),
                     t.StructField('region', t.StringType(), True),
                     t.StructField('language', t.StringType(), True),
                     t.StructField('types', t.StringType(), True),
                     t.StructField('attributes', t.StringType(), True),
                     t.StructField('isOriginalTitle', t.IntegerType(), True),
                      ])
    return schema

def schema_name_basics():
    schema=t.StructType([t.StructField('nconst', t.StringType(), True),
                     t.StructField('primaryName', t.StringType(), True),
                     t.StructField('birthYear', t.IntegerType(), True),
                     t.StructField('deathYear', t.StringType(), True),
                     t.StructField('primaryProfession', t.StringType(), True),
                     t.StructField('knownForTitles', t.StringType(), True),
                    ])
    return schema

def schema_title_basics():
    schema=t.StructType([t.StructField('tconst', t.StringType(), True),
                     t.StructField('titleType', t.StringType(), True),
                     t.StructField('primaryTitle', t.StringType(), True),
                     t.StructField('originalTitle', t.StringType(), True),
                     t.StructField('isAdult', t.IntegerType(), True),
                     t.StructField('startYear', t.StringType(), True),
                     t.StructField('endYear', t.StringType(), True),
                     t.StructField('runtimeMinutes', t.IntegerType(), True),
                     t.StructField('genres', t.StringType(), True),
                    ])
    return schema

def schema_title_principals():
    schema=t.StructType([t.StructField('tconst', t.StringType(), True),
                     t.StructField('ordering', t.IntegerType(), True),
                     t.StructField('nconst', t.StringType(), True),
                     t.StructField('category', t.StringType(), True),
                     t.StructField('job', t.StringType(), True),
                     t.StructField('characters', t.StringType(), True),
                     ])
    return schema

def schema_title_episode():
    schema=t.StructType([t.StructField('tconst', t.StringType(), True),
                     t.StructField('parentTconst', t.StringType(), True),
                     t.StructField('seasonNumber', t.IntegerType(), True),
                     t.StructField('episodeNumber', t.IntegerType(), True),
                     ])
    return schema

def schema_title_ratings():
    schema=t.StructType([t.StructField('tconst', t.StringType(), True),
                     t.StructField('averageRating', t.FloatType(), True),
                     t.StructField('numVotes', t.IntegerType(), True),
                     ])
    return schema

def schema_title_crew():
    schema=t.StructType([t.StructField('tconst', t.StringType(), True),
                     t.StructField('directors', t.StringType(), True),
                     t.StructField('writers', t.StringType(), True),
                     ])
    return schema