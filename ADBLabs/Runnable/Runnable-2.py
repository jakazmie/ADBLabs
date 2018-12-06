# Databricks notebook source
dbutils.widgets.text("date", "11-27-2013")
dbutils.widgets.text("dest_path", "tmp/raw_logs.parquet")

# COMMAND ----------

date = dbutils.widgets.get("date").strip()
dest_path = dbutils.widgets.get("dest_path").strip()

print("date", date)
print("dest_path", dest_path)

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({
    "status": "OK",
    "date": date,
    "path": dest_path
}))