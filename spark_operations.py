import pyspark
from delta import configure_spark_with_delta_pip
import pandas as pd
from dotenv import load_dotenv
import os

def chceck_hadoop_azure(spark):
    check = 1
    try:
        spark._jvm.Class.forName("org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        print("hadoop-azure is available")
    except Exception as e:
        print("hadoop-azure is NOT available")
        check = 0
        
    return check

def chceck_CloudStorageAccount(spark):
    check = 1
    try:
        spark._jvm.Class.forName("com.microsoft.azure.storage.CloudStorageAccount")
        print("azure-storage is available")
    except Exception as e:
        print("azure-storage is NOT available")
        check = 0
        
    return check



class sparkDelta():
    def __init__(self) -> None:
        load_dotenv()
        self.storage = os.environ['storageAccount']
        self.storageKey = os.environ['storageKey']
        self.spark = self.get_sparkContext()
        self.blobPath = f'wasbs://deltastorage@{self.storage}.blob.core.windows.net'

    
    def get_sparkContext(self):

        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6")
            
        # added hadoop-azure:3.3.4,azure-storage:8.6.6,jetty-util:9.4.48.v20220622,jetty-util-ajax:9.4.48.v20220622
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        if 0 in (chceck_hadoop_azure(spark), chceck_CloudStorageAccount(spark)):
            raise Exception("Install hadoop-azure:3.3.4,azure-storage:8.6.6") 
        

        spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
        spark.conf.set(
                        f'fs.azure.account.key.{self.storage}.blob.core.windows.net',
                        self.storageKey
                        )

        return spark

    def saveDeltaTable(self, data, tableName, mode = 'overwrite'):
        if isinstance(data, pd.DataFrame):
            data = self.spark.createDataFrame(data)
        data.write.format("delta").mode(mode).save(f'{self.blobPath}/{tableName}')

    def readDeltaTable(self, tableName):
        #data.write.format("delta").save(f'{self.blobPath}/{tableName}')
        spark_df = self.spark.read.format("delta").load(f'{self.blobPath}/{tableName}')
        return spark_df
        

    def sparkStop(self):
        self.spark.stop()
        
if __name__=="__main__":
    pass