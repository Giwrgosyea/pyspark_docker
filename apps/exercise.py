from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,FloatType,TimestampType,DateType	
from pyspark.sql.functions import concat_ws
import pyspark

def init_spark():
  sql = SparkSession.builder\
    .appName("ex-app")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def main():
    file = "/opt/spark-data/cvas_data_transactions.csv"
    sql,sc = init_spark()
    sql_context = pyspark.SQLContext(sc)


    schema = StructType([
            StructField("transactionDate", TimestampType(), True),
            StructField("sub_id", StringType(), True),
            StructField("amount", FloatType(), True),
            StructField("channel", StringType(), True),

        ])

    df_transcations = sql.read.csv(file,header=False,schema=schema).na.drop()
    df_transcations = df_transcations.filter(df_transcations.channel == 'SMS')


    file = "/opt/spark-data/subscribers.csv"
    schema = StructType([
            StructField("sub_id", StringType(), True),
            StructField("activationDate", DateType(), True),
        ])

    df_subs = sql.read.csv(file,header=False,schema=schema).na.drop()

    merged=df_transcations.join(df_subs,["sub_id"])
    df2=merged.select(concat_ws('_',merged.sub_id,merged.activationDate)
                .alias("row_key"),"sub_id","activationDate").selectExpr("row_key as row_key", "sub_id as sub_id","activationDate as act_dt")
    df2.write.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://mongodb_container:27017/local.task").option("replaceDocument", "true").mode("append").save()

    ## load again df2 ...
    df2_loaded = sql_context.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://mongodb_container:27017/local.task").load()

    merged=df_transcations.join(df2_loaded,["sub_id"])
    merged= merged.select(merged.transactionDate,merged.sub_id,merged.amount,merged.channel,merged.row_key)
    merged.coalesce(1).write.mode("overwrite").parquet('hdfs://namenode:9000/merged.parquet')

  
if __name__ == '__main__':
  main()