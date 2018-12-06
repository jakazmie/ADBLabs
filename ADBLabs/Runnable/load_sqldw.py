# Databricks notebook source
dbutils.widgets.removeAll()

# COMMAND ----------

# Configure job parameters

# Storage account
dbutils.widgets.text("ACCESS_KEY", "k0sEc3OL07/c5Gy5L4LS4bPrvczX8Smktn2GGpISa9iQ4CGdPRvPQXZ71ZbAg5K3YCXpBJnk1kV/+ZahmO2KCA==019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D")


dbutils.widgets.text("SAS_KEY", "?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D")

dbutils.widgets.text("STORAGE_ACCOUNT", "azureailabs")

# Input container
dbutils.widgets.text("CONTAINER", "chicago-crime")

# Staging container
dbutils.widgets.text("OUTPUT_CONTAINER", "stage")

# SQL DW
dbutils.widgets.text("JDBCHOSTNAME", "jksqldbserver.database.windows.net")
dbutils.widgets.text("JDBCDATABASE", "jksqldw")
dbutils.widgets.text("JDBCPORT", "1433")
dbutils.widgets.text("OUTPUT_TABLE", "chicagocrimes")
dbutils.widgets.text("USER", "")
dbutils.widgets.text("PASSWORD", "")


# COMMAND ----------

# Set up connection to Azure Blob Storage
SAS_KEY = dbutils.widgets.get("SAS_KEY").strip()
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT").strip()
SAS_KEY = dbutils.widgets.get("SAS_KEY").strip()
CONTAINER = dbutils.widgets.get("CONTAINER").strip()


source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
conf_key = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

spark.conf.set(conf_key, SAS_KEY)

# COMMAND ----------

# Load CSV file to DataFrame
source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

crimeDF = (spark.read
                  .option("header", True)
                  .option("delimiter", "\t")
                  .option("timestampFormat", "mm/dd/yyyy hh:mm:ss a")
                  .option("inferSchema", True)
                  .csv(source_str))

# COMMAND ----------

# Rename the columns in `crimeDF` so there are no spaces or invalid characters

cols = crimeDF.columns
titleCols = [''.join(j for j in i.title() if not j.isspace()) for i in cols]
camelCols = [column[0].lower()+column[1:] for column in titleCols]

crimeRenamedColsDF = crimeDF.toDF(*camelCols)

# COMMAND ----------

OUTPUT_CONTAINER = dbutils.widgets.get("OUTPUT_CONTAINER").strip()
JDBCHOSTNAME = dbutils.widgets.get("JDBCHOSTNAME").strip()
JDBCPORT = dbutils.widgets.get("JDBCPORT").strip()
JDBCDATABASE = dbutils.widgets.get("JDBCDATABASE").strip()
USER = dbutils.widgets.get("USER").strip()
# Configure connection string and URLs

OUTPUT_TABLE = dbutils.widgets.get("OUTPUT_TABLE").strip()
PASSWORD = dbutils.widgets.get("PASSWORD").strip()

staging_dir = "wasbs://{}@{}.blob.core.windows.net/".format(OUTPUT_CONTAINER, STORAGE_ACCOUNT)
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(JDBCHOSTNAME, JDBCPORT, JDBCDATABASE, USER, PASSWORD)

print(staging_dir)
print(jdbcUrl)

# COMMAND ----------

# Save the dataframe to SQL DW
(crimeRenamedColsDF.write 
   .format("com.databricks.spark.sqldw")
  .option("url", jdbcUrl)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", OUTPUT_TABLE)
  .option("tempDir", staging_dir)
  .save())

# COMMAND ----------

dbutils.notebook.exit("Notebook successfully executed. Wrote CSV files in {} to a parquet file at {}".format(source_str, OUTPUT_PATH))