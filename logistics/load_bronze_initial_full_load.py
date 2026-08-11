# Databricks notebook source
# import all classes and functions from pyspark sql module
from pyspark.sql import *
# import all the function build_in pyspark sql functions use them directly in pyspark code  
from pyspark.sql.functions import *
# imports all the data types and schema-related classes from PySpark
from pyspark.sql.types import *
# import python regular expression (regax) module [It is used when you want to search, find, extract, replace, or validate patterns in text.]
import re
# creates a variable named "access_key" to store storage access key to avoid sensitive credentials in the notebook
access_key = dbutils.secrets.get(scope="project-scope",key="adlsdevfilesstorageacAccesskey")
# authentication gives to the spark to read and write from the ADLS Gen2 storage ac using access key.
spark.conf.set("fs.azure.account.key.adlsdevfilesstorageac.dfs.core.windows.net",access_key)
# used to list the files and directories in a given adls gen2 storage account
dbutils.fs.ls("abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/Raw/In/Sales_Data_Prior_Day")
# creates a varialbe "folderPath" to store the adls gen2 folder path for further use
folderPath = "abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net"
# dbutils.secrets.get() is used to securely retrieve a secret from a Databricks Secret Scope. Here, project-scope is the scope and servername is the key. The retrieved server name is stored in the serverName variable and can then be used for database connectivity.

serverName = dbutils.secrets.get(scope='project-scope',key='servername')
databaseName = dbutils.secrets.get(scope='project-scope',key='databasename')
userName = dbutils.secrets.get(scope='project-scope',key='username')
password = dbutils.secrets.get(scope='project-scope',key='password')


configDF = spark.read \
  .format("jdbc") \
  .option("url",f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("dbtable", "metadata.OBJECTS_CONFIGURATION") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()
display(configDF)


# COMMAND ----------

# get a perticular column data
for row in configDF.collect():
    print(row["SOURCE_FILE_PATH"])

# COMMAND ----------

#concatination
for Row in configDF.collect():
    print(folderPath+'/'+Row["SOURCE_FILE_PATH"]+'/'+Row["SOURCE_FILE_NAME"]+'_*.csv')

# COMMAND ----------

# DBTITLE 1,Cell 12
#read the csv file of appointmentTest
df1 = spark.read \
    .format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/Raw/In/AppointmentDataTest/AppointmentDataTest_19072026.csv")
display(df1)

#read the csv file of sales data
df2 = spark.read \
    .format('csv')\
        .option("header",True)\
        .option("inferSchema",True)\
        .load("abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/Raw/In/Sales_Data_Prior_Day/Sales_Data_Prior_Day_19072026.csv")
display(df2)
        

# COMMAND ----------

print(df2.columns)

# COMMAND ----------

# Check what folders exist under Raw/In
dbutils.fs.ls("abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/Raw/In/AppointmentDataTest/")

# COMMAND ----------

#print the columns in a list format
for column in df2.columns:
    print(column)

# COMMAND ----------

def load_data_into_delta(ObjectName, filePath):
    df = spark.read.format("csv").option("header", True).load(filePath)
    for column_name in df.columns:
        new_column_name = re.sub(r"[^a-zA-Z0-9]+", "_", column_name).strip("_")
        new_column_name = new_column_name.upper()
        df = df.withColumnRenamed(column_name, new_column_name)
    print(df.columns)
    df = df.withColumn("Added_By", lit(masterPipelineRunID)) \
           .withColumn("Added_On", current_timestamp())\
           .withColumn("Modified_By",lit(masterPipelineRunID))\
         .withColumn("Modified_On", current_timestamp())
    target_df = spark.table(ObjectName)
    print(df.columns)
    for field in target_df.schema.fields:
        print(field.name)
        if field.name in df.columns:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    df = df.select(*target_df.columns)
    display(df)
    df.write.format("delta").mode("append").saveAsTable(ObjectName)

# COMMAND ----------

for Row in configDF.collect():
    print(folderPath + '/'+ Row["SOURCE_FILE_PATH"]+ '/'+ Row["SOURCE_FILE_NAME"]+'_*.csv')

# COMMAND ----------

dbutils.widgets.text("masterPipelineRunID", "")
masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

# COMMAND ----------

# DBTITLE 1,Cell 24
#move file to archive folder


# try:
#     for row in configDF.collect():
#         ObjectName = row['BRONZE_TABLE_NAME']
#         filePath = row['SOURCE_FILE_PATH']
#         load_data_into_delta(ObjectName, folderPath + '/' + filePath + '/*.csv')
# except Exception as e:
#     print(f"Error occurred: {e}")
#     raise

# COMMAND ----------

try:
    for row in configDF.collect():
        ObjectName = row['BRONZE_TABLE_NAME']
        filePath = row['SOURCE_FILE_PATH']
        for path in dbutils.fs.ls(folderPath + '/' + filePath):
            load_data_into_delta(ObjectName, path.path)
except Exception as e:
    print(f"Error occurred: {e}")
    raise

# COMMAND ----------

# # Move File to archive folder
# try:
#     for row in configDF.collect():
#             filePath = row['SOURCE_FILE_PATH']
#             archivePath = row['ARCHIVE_PATH']
#             sourcePath = f"abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/{filePath}/"
#             archivePath = f"abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/{archivePath}/archive"
#             for f in dbutils.fs.ls(sourcePath):
#                 if f.name.endswith(".csv"):
#                     dbutils.fs.mv(f.path, archivePath + '/' + f.name)
# except Exception as e:
#     print(f"Error occurred: {e}")
#     raise

# COMMAND ----------

dbutils.fs.ls(folderPath)

# COMMAND ----------

configDF.select("SOURCE_FILE_PATH", "ARCHIVE_PATH").show(truncate=False)

# COMMAND ----------

# print(filePath)
# print(archivePath)
sourcePath = f"abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net/{filePath}/"
print(sourcePath)

# COMMAND ----------

dbutils.fs.ls(sourcePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze.logistic.appointment_data
# MAGIC WHERE Added_by = '97648994-a97e-4dd2-bedb-2d3b136baf46';