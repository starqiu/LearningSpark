from pyspark.sql import SparkSession, DataFrame
import pandas as pd

class IOHelper(object):

    def __init__(self):
        session = SparkSession.builder.appName("Python Spark SQL basic example").enableHiveSupport().getOrCreate()
        self.sparkSession = session

    def getSparkSession(self):
        return self.sparkSession

    def read(self, tableName="train"):
        self.sparkSession.table(tableName)

    def write(self, df: DataFrame, tableName="result"):
        df.write.mode("overwrite").saveAsTable(tableName)


    def getFromPadasDF(self, pddf: pd.DataFrame):
        self.sparkSession.createDataFrame(pddf).toPandas