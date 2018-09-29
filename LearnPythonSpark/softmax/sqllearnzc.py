#!/usr/bin/env python3

# -*- coding:utf-8 -*-
from __future__ import print_function

from os.path import expanduser, join

from pyspark.sql import SparkSession

import pandas as pd

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL Hive integration example") \
        .enableHiveSupport() \
        .getOrCreate()

    #df = spark.sql("show databases")

    spark.sql("use industry")
    

    df = spark.sql("select * from datas")
    df_test = pd.DataFrame(df.toPandas(),dtype='str')

    df2= spark.createDataFrame(df_test)
    df2.write.mode("overwrite").saveAsTable("test")
    df3 = spark.sql("select * from test")
    df3.head
    df4 = df3.toPandas()
    print(df4.dtypes)
    #print(df.toPandas().head())
    #df1 = spark.read.table("vulcanus_load.dfk_ry_ajdy")
    #df1.write.csv("file:////usr/local/envTech/algorithm/zxf", "append")
    #df.write.saveAsTable("datas")

    df_test = df.toPandas()
    df_test.dtypes

    spark.stop()

