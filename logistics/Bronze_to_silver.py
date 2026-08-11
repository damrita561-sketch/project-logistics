# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# COMMAND ----------

# From the project-scope secure folder, get the values and stored under these four keys
serverName = dbutils.secrets.get(scope='project-scope',key='servername')
databaseName = dbutils.secrets.get(scope='project-scope',key='databasename')
userName = dbutils.secrets.get(scope='project-scope',key='username')
password = dbutils.secrets.get(scope='project-scope',key='password')

# COMMAND ----------

# creating a dataframe and read configuration data from azure sql db into pyspark using jdbc connections 
configSilverDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("query", "select * from metadata.OBJECTS_CONFIGURATION where IS_RUN = 1") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# databricks utilities used to visually display a dataframe into a interactive tabular format
display(configSilverDF)

# COMMAND ----------

# columnMappingDF reads the metadata.OBJECTS_COLUMN_MAPPING table from SQL Server into a PySpark DataFrame using JDBC. The connection details are provided through the JDBC URL, username, password, and SQL Server driver. Since dbtable is used, Spark directly reads the specified table
columnMappingDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
  .option("dbtable", "metadata.OBJECTS_COLUMN_MAPPING") \
  .option("user",userName) \
  .option("password", password) \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()


# COMMAND ----------

# shows that columnMappingDF dataframe in a readable table format in the Databricks notebook.
display(columnMappingDF)

# COMMAND ----------

#Creating the Temperory View for the session so to use sql query further.
columnMappingDF.createOrReplaceTempView("fieldMappingView")

# COMMAND ----------

# filetering the columnMappingDf based on the object name.get only columnmapping recors where object name is "SALES_DATA_PRIOR_DATA"
df = columnMappingDF.filter(col("OBJECT_NAME")=="SALES_DATA_PRIOR_DAY")

display(df)

# COMMAND ----------

df = columnMappingDF.filter(col("OBJECT_NAME")== "SALES_DATA_PRIOR_DAY").select("SOURCE_COLUMN_NAME")
df1 = columnMappingDF.filter(col("OBJECT_NAME")== "SALES_DATA_PRIOR_DAY").select("TARGET_COLUMN_NAME")

display(df)
display(df1)

# COMMAND ----------

# convert df to list
print(df.collect())

# COMMAND ----------

# get cleaned data
df = columnMappingDF.filter(col("OBJECT_NAME")== "SALES_DATA_PRIOR_DAY").select("SOURCE_COLUMN_NAME").rdd.flatMap(lambda x:x ).collect()
df1 = columnMappingDF.filter(col("OBJECT_NAME")== "SALES_DATA_PRIOR_DAY").select("TARGET_COLUMN_NAME").rdd.flatMap(lambda x:x ).collect()

print(df)
print(df1)

# COMMAND ----------

# convert to dictionary
renameDict = dict(zip(df,df1))
print(renameDict)

# COMMAND ----------

for old_col,new_col in renameDict.items():
    print(old_col,"------->",new_col)

# COMMAND ----------

def renaming_column_name (ObjectName : str,rawDF : DataFrame) :
    sourceColList = columnMappingDF.filter(col("OBJECT_NAME") == ObjectName).select ("SOURCE_COLUMN_NAME").rdd.flatMap (lambda x:x).collect()
    targetColList = columnMappingDF.filter(col("OBJECT_NAME")== ObjectName).select ("SOURCE_COLUMN_NAME").rdd.flatMap (lambda x:x).collect()
    renameDict = dict(zip(sourceColList.targetColList))
    bronzeDF = rawDF
    for old_col,new_col in renameDict.items():
        bronzeDF = rawDF.withColumnRenamed(old_col,new_col)
    return bronzeDf

# COMMAND ----------

def create_column_mapping_dict(object_name, column_mapping_df):
    source_cols = columnMappingDF.filter(col("OBJECT_NAME") == f'(objectName)').select("SOURCE_COLUMN_NAME").rdd.flatMap(lambda x:x).collect()
    target_cols = columnMappingDF.filter(col("OBJECT_NAME")== f'(objectName)').select("TARGET_COLUMN_NAME").rdd.flatMap(lambda x:x).collect()
    column_mapping_dict = dict(zip(source_cols,target_cols))
    return column_mapping_dict

# COMMAND ----------

# displays the rows ehere VALUES is notnull
df = columnMappingDF.filter(col("VALUES").isNotNull())
display(df)

# COMMAND ----------

     # keeps the rows where values columns is not null
df = columnMappingDF.filter((col("VALUES").isNotNull())
         &
         # keep the rows where object name is SALES_DATA_PRIOR_DAY 
         (col("OBJECT_NAME") == "SALES_DATA_PRIOR_DAY"))
display(df)

# COMMAND ----------

df = columnMappingDF.filter((col("VALUES").isNotNull()) & (col("OBJECT_NAME") == "SALES_DATA_PRIOR_DAY")).select ("TARGET_COLUMN_NAME")
df1 = columnMappingDF.filter((col("VALUES").isNotNull()) & (col("OBJECT_NAME") == "SALES_DATA_PRIOR_DAY")).select ("VALUES")
display(df)
display(df1)

# COMMAND ----------

def null_handlings(ObjectName,rawDF):
    targetColumnList = columnMappingDF.filter((col("VALUES").isNotNull()) & (col("OBJECT_NAME") == "ObjectName")).select ("TARGET_COLUMN_NAME").rdd.flatMap(lambda x:x). collect()
    nullValueList = columnMappingDF.filter((col("VALUES").isNotNull()) & (col("OBJECT_NAME") == "ObjectName")).select ("VALUES").rdd.flatMap(lambda x:x).collect()
    handleNullsDict = dict(zip(targetColumnList,nullValueList))
    bronzeDF = rawDF
    for column,value in handleNullsDict.items():
        bronzeDF = rawDF.fillna({column:value})
    return bronzeDF

# COMMAND ----------

# keeps the rows where the objectname is "SALES_DATA_PRIOR_DAY" and from select rows select only one column "TARGET_COLUMN_NAME"
df = columnMappingDF.filter(col("OBJECT_NAME") == "SALES_DATA_PRIOR_DAY") . select ("TARGET_COLUMN_NAME")
#
df1 =columnMappingDF.filter(col("OBJECT_NAME") == "SALES_DATA_PRIOR_DAY") . select ("COLUMN_DATATYPE") 
display(df)
display(df1)

# COMMAND ----------

# DBTITLE 1,Cast columns to metadata-defined data types
def data_type_casting(ObjectName,columnMappingDF,rawDF):

    df = columnMappingDF.filter(col("OBJECT_NAME") == ObjectName) . select ("TARGET_COLUMN_NAME").rdd.flatMap(lambda x:x).collect()
    df1 =columnMappingDF.filter(col("OBJECT_NAME") == ObjectName) . select ("COLUMN_DATATYPE").rdd.flatMap(lambda x:x).collect()
    datatypecastingdict  = dict(zip(df,df1))
    bronzeDF = rawDF
    for col_name,col_type in datatypecastingdict.items():
        bronzeDF = rawDF.withColumn (col_name,col(col_name).cast(col_type))
    return bronzeDF

    display(bronzeDF)

# COMMAND ----------

rows = spark.sql (f""" 
                SELECT TARGET_COLUMN_NAME
                FROM  fieldMappingView
                WHERE lower(SOURCE_NAME) = lower('logistic')
                AND lower (OBJECT_NAME)= lower("SALES_PRIOR_DAY")
                        ORDER BY TARGET_COLUMN_NAME
                        """).collect()
print(rows)


# COMMAND ----------

rows = spark.sql (f""" 
                SELECT TARGET_COLUMN_NAME
                FROM  fieldMappingView
                WHERE lower(SOURCE_NAME) = lower('logistic')
                AND lower (OBJECT_NAME)= lower("SALES_PRIOR_DAY")
                        ORDER BY TARGET_COLUMN_NAME
                        """).collect()
print(rows)

# COMMAND ----------

rows = spark.sql (f""" 
                SELECT TARGET_COLUMN_NAME
                FROM  fieldMappingView
                WHERE lower(SOURCE_NAME) = lower('logistic')
                AND lower (OBJECT_NAME)= lower("SALES_PRIOR_DAY")
                        ORDER BY TARGET_COLUMN_NAME
                        """).collect()
print(rows)

# COMMAND ----------

# DBTITLE 1,Cell 28
def getColumnsForMerge(sourceName, ObjectName, sourceAlias="s", targetAlias="t", skipCols=None):
    if skipCols is None:
        skipCols = ["Added_On", "Added_By", "Modified_On", "Modified_By"]

    rows = spark.sql(f"""
                SELECT DISTINCT TARGET_COLUMN_NAME
                FROM fieldMappingView
                WHERE lower(SOURCE_NAME) = lower('{sourceName}')
                AND lower(OBJECT_NAME) = lower('{ObjectName}')
                            ORDER BY TARGET_COLUMN_NAME
                        """).collect()
    
    if not rows:
        raise ValueError(f"No mappings found for {sourceName} -> {ObjectName}")

    src_columns = []
    tgt_columns = []
    merge_pairs = []

    for r in rows:
        dest_col = r["TARGET_COLUMN_NAME"]
        src_col  = r["TARGET_COLUMN_NAME"]

        if dest_col in skipCols:
            continue

        src_columns.append(f"{sourceAlias}.{src_col}")
        tgt_columns.append(dest_col)
        merge_pairs.append(f"{targetAlias}.{dest_col} = {sourceAlias}.{src_col}")

         # audit_fields = ["Added_On", "Added_By", "Modified_On", "Modified_By"]
    target_columns_full = tgt_columns 

    # merge_pairs.append(f"{targetAlias}.ADDED_ON = {sourceAlias}.ADDED_ON")
    # merge_pairs.append(f"{targetAlias}.ADDED_BY = {sourceAlias}.ADDED_BY")

    sourceColumn = ", ".join(src_columns)
    targetColumn = ", ".join(target_columns_full)
    mergeColumnStatement = ", ".join(merge_pairs)
    targetColumn = targetColumn + ", ADDED_BY, ADDED_ON, MODIFIED_BY, MODIFIED_ON"

    return sourceColumn, targetColumn, mergeColumnStatement

# COMMAND ----------

def load_data_into_silver(sourceName,objectName ,targetTableName, bronzeDF, operationType, keyColumnName,isFullLoad,masterPipelineRunID):
    bronzeDF.createOrReplaceTempView("tempView")
    srcColumns,targetColumns,mergeColumnStatement = getColumnsForMerge(sourceName, objectName, sourceAlias="s", targetAlias="t", skipCols=None)
    if operationType.lower() == "upsert":
        if isFullLoad == 1:
            spark.sql(f"DELETE FROM {targetTableName}")
            spark.sql(f"INSERT INTO {targetTableName} ({targetColumns}) SELECT {srcColumns} FROM tempView s")
            df_history = spark.sql(f"describe history {targetTableName} limit 1").first()
            rowsInserted = df_history["operationMetrics"]["numOutputRows"]
            print("Number of Rows Inserted :", rowsInserted)
        else:
            mergeCondition = ""
            for key in keyColumnName.split(","):
                key = key.strip()
                mergeCondition += f" AND s.{key}=t.{key}" if mergeCondition else f"s.{key}=t.{key}"
            mergeQuery = f"""
                MERGE INTO {targetTableName} t
                USING tempView s
                ON {mergeCondition}

                WHEN MATCHED THEN UPDATE SET
                    {mergeColumnStatement},
                    t.Modified_On = CURRENT_TIMESTAMP,
                    t.Modified_By = '{masterPipelineRunID}'
                WHEN NOT MATCHED THEN INSERT ({targetColumns})
                VALUES ({srcColumns},'{masterPipelineRunID}',CURRENT_TIMESTAMP,'{masterPipelineRunID}',CURRENT_TIMESTAMP)
            """
            print(f"Executing merge query for {objectName}")
            spark.sql(mergeQuery)
    

# COMMAND ----------

# DBTITLE 1,Cell 31
dbutils.widgets.text("masterPipelineRunID", "")
masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

try:
    for row in configSilverDF.collect():
            ObjectName = row['OBJECT_NAME']
            sourceName = row['SOURCE_NAME']
            targetTableName = row['SILVER_TABLE_NAME']
            operationType = row['OPERATION_TYPE']
            keyColumnName = row['KEY_COLUMN_NAME']
            isFullLoad = row['IS_FULL_LOAD']
            bronzeTableName = row['BRONZE_TABLE_NAME']
            bronzeDF = spark.table(f"{bronzeTableName}")
            objectdict = dict(zip(
                columnMappingDF.filter(col("OBJECT_NAME") == ObjectName).select("SOURCE_COLUMN_NAME").rdd.flatMap(lambda x: x).collect(),
                columnMappingDF.filter(col("OBJECT_NAME") == ObjectName).select("TARGET_COLUMN_NAME").rdd.flatMap(lambda x: x).collect()
            ))
            for old_col,new_col in objectdict.items():
                if old_col in bronzeDF.columns and old_col != new_col:
                    if new_col in bronzeDF.columns:
                        bronzeDF = bronzeDF.drop(new_col)
                    bronzeDF = bronzeDF.withColumnRenamed(old_col, new_col)
            # Workaround: dedupe on the merge key to avoid DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE
            # when the bronze source has duplicate rows for the same key.
            dedupeKeyCols = [k.strip() for k in keyColumnName.split(",")]
            bronzeDF = bronzeDF.dropDuplicates(dedupeKeyCols)
            null_handlings(ObjectName, bronzeDF)
            data_type_casting(ObjectName, columnMappingDF, bronzeDF)
            load_data_into_silver(sourceName,ObjectName ,targetTableName, bronzeDF, operationType, keyColumnName,isFullLoad,masterPipelineRunID)
except Exception as e:
    print(f"Error occurred: {e}")
    raise