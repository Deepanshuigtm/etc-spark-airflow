##import required libraries
import pyspark

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "/Users/deepanshu/Downloads/postgresql-42.7.3.jar") \
   .getOrCreate()


##read table from db using spark jdbc
movies_df = spark.read \
   .format("jdbc") \
   .option("url", "jdbc:postgresql://localhost:5432/etl_pipeline") \
   .option("dbtable", "movies") \
   .option("user", "postgres") \
   .option("password", "newpassword") \
   .option("driver", "org.postgresql.Driver") \
   .load()

##print the movies_df
print(movies_df.show())




