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