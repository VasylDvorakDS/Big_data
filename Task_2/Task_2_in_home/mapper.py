from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

def mapper(path):
    spark = SparkSession.builder.getOrCreate()
  
    df = spark.read.option("header", "true").csv(path).select('price')
    df = df.filter(col("price").isNotNull())
    df = df.filter(col("price").rlike("^[0-9]+(\.[0-9]*)?$"))

    pandas_df = df.toPandas()  # Преобразуем только если это PySpark DataFrame

    pandas_df['price'] = pd.to_numeric(pandas_df['price'], errors='coerce').astype('float32')

    return pandas_df
