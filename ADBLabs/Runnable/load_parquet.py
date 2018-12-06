# Databricks notebook source
# Configure input parameters

dbutils.widgets.text("SAS_KEY", "?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D")
dbutils.widgets.text("STORAGE_ACCOUNT", "azureailabs")
dbutils.widgets.text("CONTAINER", "chicago-crime")
dbutils.widgets.text("OUTPUT_PATH", "/datasets/chicagocrimes.parquet")

# COMMAND ----------

# Set up connection to Azure Blob Storage
SAS_KEY = dbutils.widgets.get("SAS_KEY").strip()
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT").strip()
SAS_KEY = dbutils.widgets.get("SAS_KEY").strip()
CONTAINER = dbutils.widgets.get("CONTAINER").strip()
OUTPUT_PATH = dbutils.widgets.get("OUTPUT_PATH").strip()

source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
conf_key = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

spark.conf.set(conf_key, SAS_KEY)

# COMMAND ----------

# Load CSV file to DataFrame
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

crimeRenamedColsDF.write.mode("overwrite").parquet(OUTPUT_PATH)

# COMMAND ----------

dbutils.notebook.exit("Notebook successfully executed. Wrote CSV files in {} to a parquet file at {}".format(source_str, OUTPUT_PATH))