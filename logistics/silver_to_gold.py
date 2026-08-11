# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# This line creates a text widget (an input box) in your Databricks notebook
dbutils.widgets.text("masterPipelineRunID", "")

# COMMAND ----------


masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")
print(masterPipelineRunID)

# COMMAND ----------

masterPipelineRunID = dbutils.widgets.get("masterPipelineRunID")

# COMMAND ----------

masterpipelineRunID = '1234'

# COMMAND ----------

# use to read data from azure sql database to pyspark using jdbc connections
# read data from an external source and stores in dataframe name 'confihGoldDF
# tells that the data source is database
 # database connection url
   # instead of reading the entire table pyaprk reads only the query
configGoldDF = spark.read \
  .format("jdbc") \
  .option("url", f"jdbc:sqlserver://projectdevsqlserver.database.windows.net:1433;database=projectdevsqldb") \
  .option("query", "select * from metadata.EDW_CONFIG where IS_RUN=1") \
  .option("user",'user') \
  .option("password", 'Welcome@123') \
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
  .load()

# COMMAND ----------

display(configGoldDF)

# COMMAND ----------

def getColumnsForMerge(ObjectName, sourceAlias="s", targetAlias="t", skipCols=None):
    if skipCols is None:
        skipCols = [ "MODIFIED_ON", "MODIFIED_BY"]

    row = (
        configGoldDF
        .filter(col("OBJECT_NAME") == ObjectName).orderBy("GOLD_COLUMN_NAMES")
        .select("GOLD_COLUMN_NAMES")
        .first()
    )

    if not row:
        raise ValueError(f"No mappings found for {ObjectName}")

    cols = [c.strip() for c in row["GOLD_COLUMN_NAMES"].split(",")]
    
    src_columns = []
    tgt_columns = []
    merge_pairs = []

    for c in cols:
        src_columns.append(f"{sourceAlias}.{c}")
        tgt_columns.append(c)

        if c not in skipCols and c != "ACCOUNT_KEY":
            merge_pairs.append(f"{targetAlias}.{c} = {sourceAlias}.{c}")
    cols_to_remove = ["ADDED_BY","ADDED_ON","MODIFIED_BY", "MODIFIED_ON"]
    for col_name in cols_to_remove:
        aliased_col = f"{sourceAlias}.{col_name}"
        if aliased_col in src_columns:
            src_columns.remove(aliased_col)
    sourceColumn = ", ".join(src_columns)
    targetColumn = ", ".join(tgt_columns)
    mergeColumnStatement = ", ".join(merge_pairs)
    insertSourceColumn = sourceColumn + ", s.ADDED_BY, s.ADDED_ON, s.MODIFIED_BY, s.MODIFIED_ON"
    
    return sourceColumn, insertSourceColumn,targetColumn, mergeColumnStatement

# COMMAND ----------

# DBTITLE 1,Cell 9
def load_data_into_gold(objectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID):
    sourceName = 'logistic'
    finalDF.createOrReplaceTempView("tempView")
    srcColumns,insertSourceColumn,targetColumns,mergeColumnStatement = getColumnsForMerge(ObjectName, sourceAlias="s", targetAlias="t", skipCols=None)
    if operationType.lower() == "upsert":
            if isFullLoad == 1:
                print(f"FULL LOAD STARTED FOR {objectName}")
                delete_query = spark.sql(f"DELETE FROM {targetCatalogName}.{targetSchemaName}.{targetTableName}")
                insert_query = spark.sql(f"""
                                    INSERT INTO {targetCatalogName}.{targetSchemaName}.{targetTableName}
                                    SELECT * FROM tempView
                                    """)
                df_history = spark.sql(f"describe history {targetCatalogName}.{targetSchemaName}.{targetTableName} limit 1").first()
                rowsInserted = df_history["operationMetrics"]["numOutputRows"]
                print("Number of Rows Inserted :", rowsInserted)
                print(f"Data Loading for {objectName} completed")
            else:
                print(f"Merge Operation Started on {targetTableName}")
                mergeQuery = f"""
                MERGE INTO {targetCatalogName}.{targetSchemaName}.{targetTableName} t
                USING tempView s ON s.{keyColumnName}=t.{keyColumnName}
                WHEN MATCHED THEN UPDATE
                SET {mergeColumnStatement},Modified_On = CURRENT_TIMESTAMP, Modified_By = '{masterPipelineRunID}'
                WHEN NOT MATCHED THEN INSERT ({targetColumns}) VALUES ({srcColumns},CURRENT_TIMESTAMP,'{masterPipelineRunID}', CURRENT_TIMESTAMP, '{masterPipelineRunID}')
                """
                print("Executing merge query")
                spark.sql(mergeQuery)
                print(f"Data Loading for {objectName} completed")

# COMMAND ----------

def dim_appointment_data():
    appointmentDataDF = spark.read.table('silver.logistic.APPOINTMENT_DATA')\
                .select(col('YARD_NAME').alias('YARD_NAME')\
                ,col('SALES_ORDER_TICKET_ID').alias('SALES_ORDER_ID')
                ,'CARRIER_NAME')
    finalDF = appointmentDataDF.withColumn('APPOINTMENT_DATA_KEY',row_number().over(Window.orderBy(col('YARD_NAME'))))
    finalDF = finalDF.withColumn('ADDED_ON',current_timestamp())\
                     .withColumn('ADDED_BY',lit(masterPipelineRunID))\
                     .withColumn('MODIFIED_ON',current_timestamp())\
                     .withColumn('MODIFIED_BY',lit(masterPipelineRunID))
    targetTableDF = spark.read.table("gold.edw.DIM_APPOINTMENT_DATA")
    targetSchema = targetTableDF.schema
    finalDF = finalDF.select(*[ col(field.name).cast(field.dataType).alias(field.name) for field in targetSchema])
    return finalDF
    

# COMMAND ----------

# DBTITLE 1,Cell 11
def fact_sales():
    salesDF = spark.read.table('silver.logistic.SALES_DATA_PRIOR_DAY')\
                    .select(col('YARD_ID').alias('YARD_ID')\
                    ,'COMMODITY_NAME'
                    ,'PRICE'
                    ,'SELL_PRICE'
                    ,'INVOICE_TOTAL','YARD')
    dimAppointmentDataDF = spark.read.table("gold.edw.DIM_APPOINTMENT_DATA").select("YARD_NAME", "APPOINTMENT_DATA_KEY")
    finalDF = salesDF.alias("s").join(dimAppointmentDataDF.alias("da"), col("s.YARD") == col("da.YARD_NAME"), "left").select("s.*", "da.APPOINTMENT_DATA_KEY").withColumn("SALES_KEY",row_number().over(Window.orderBy("YARD_ID", "APPOINTMENT_DATA_KEY", "COMMODITY_NAME")))
    finalDF = finalDF.withColumn('ADDED_ON',current_timestamp())\
                     .withColumn('ADDED_BY',lit(masterPipelineRunID))\
                     .withColumn('MODIFIED_ON',current_timestamp())\
                     .withColumn('MODIFIED_BY',lit(masterPipelineRunID))
    targetTableDF = spark.read.table("gold.edw.FACT_SALES")
    targetSchema = targetTableDF.schema
    finalDF = finalDF.select(*[ col(field.name).cast(field.dataType).alias(field.name) for field in targetSchema])
    return finalDF

# COMMAND ----------

# DBTITLE 1,Cell 12
try:
    for row in configGoldDF.collect():
            ObjectName = row['OBJECT_NAME']
            sourceName = 'logistic'
            targetTableName = row['OBJECT_NAME']
            operationType = row['OPERATION_TYPE']
            keyColumnName = row['GOLD_KEY_COLUMN_NAME']
            targetCatalogName = row['GOLD_CATALOG_NAME']
            targetSchemaName = row['GOLD_SCHEMA_NAME']
            isFullLoad = row['IS_FULL_LOAD']
            if ObjectName.lower() == 'dim_appointment_data':
                finalDF = dim_appointment_data()
                load_data_into_gold(ObjectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID)
            elif ObjectName.lower() == 'fact_sales':
                spark.conf.set("spark.sql.ansi.enabled", "false")
                finalDF = fact_sales()
                load_data_into_gold(ObjectName ,targetTableName, finalDF, operationType, keyColumnName,targetCatalogName,targetSchemaName,    isFullLoad,masterPipelineRunID)
                spark.conf.set("spark.sql.ansi.enabled", "true")
            else:
                print(f"Invalid Object Name {ObjectName}")
except Exception as e:
    print(f"Error occurred: {e}")
    raise

# COMMAND ----------

# DBTITLE 1,Cell 13
# Load file path metadata from OBJECTS_CONFIGURATION
# configGoldDF (metadata.EDW_CONFIG) has no SOURCE_FILE_PATH or ARCHIVE_PATH columns —
# those live in metadata.OBJECTS_CONFIGURATION, same source the bronze archive step uses.
serverName   = dbutils.secrets.get(scope='project-scope', key='servername')
databaseName = dbutils.secrets.get(scope='project-scope', key='databasename')
userName     = dbutils.secrets.get(scope='project-scope', key='username')
password     = dbutils.secrets.get(scope='project-scope', key='password')

configGoldDF = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{serverName}:1433;database={databaseName}") \
    .option("query", """
        SELECT e.*, o.SOURCE_FILE_PATH, o.ARCHIVE_PATH
        FROM metadata.EDW_CONFIG e
        JOIN metadata.OBJECTS_CONFIGURATION o ON e.OBJECT_NAME = o.BRONZE_TABLE_NAME
        WHERE e.IS_RUN = 1
    """) \
    .option("user", userName) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

adlsBase = "abfss://logistics@adlsdevfilesstorageac.dfs.core.windows.net"
try:
    for row in configGoldDF.collect():
        ObjectName        = row['OBJECT_NAME']
        targetTableName   = row['OBJECT_NAME']
        operationType     = row['OPERATION_TYPE']
        keyColumnName     = row['GOLD_KEY_COLUMN_NAME']
        targetCatalogName = row['GOLD_CATALOG_NAME']
        targetSchemaName  = row['GOLD_SCHEMA_NAME']
        isFullLoad        = row['IS_FULL_LOAD']
        sourcePath  = f"{adlsBase}/{row['SOURCE_FILE_PATH']}/"
        archivePath = f"{adlsBase}/{row['ARCHIVE_PATH']}/archive"

        # ==========================
        # GOLD PROCESSING
        # ==========================

        if ObjectName.lower() == 'dim_appointment_data':
            finalDF = dim_appointment_data()
            load_data_into_gold(ObjectName, targetTableName, finalDF, operationType, keyColumnName, targetCatalogName, targetSchemaName, isFullLoad, masterPipelineRunID)
        elif ObjectName.lower() == 'fact_sales':
            spark.conf.set("spark.sql.ansi.enabled", "false")
            try:
                finalDF = fact_sales()
                load_data_into_gold(ObjectName, targetTableName, finalDF, operationType, keyColumnName, targetCatalogName, targetSchemaName, isFullLoad, masterPipelineRunID)
            finally:
                spark.conf.set("spark.sql.ansi.enabled", "true")
        else:
            print(f"Invalid Object Name {ObjectName}")

        # ==========================
        # ARCHIVE
        # ==========================

        dbutils.fs.mv(
            sourcePath,
            archivePath,
            True
        )

    print("Gold processing and archive completed successfully")

except Exception as e:

    print(f"Pipeline failed: {str(e)}")

    raise