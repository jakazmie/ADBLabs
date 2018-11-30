# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A Step Toward Full Automation
# MAGIC 
# MAGIC Notebooks are one--and not the only--way of interacting with the Spark and Databricks environment.  Notebooks can be executed independently and as recurring jobs.  They can also be exported and versioned using git.  Python files and Scala/Java jars can be executed against a Databricks cluster as well, allowing full integration with a developer's normal workflow.  Since notebooks can be executed like code files and compiled binaries, they offer a way of building production pipelines.
# MAGIC 
# MAGIC Functional programming design principles aid in thinking about pipelines.  In functional programming, your code always has known inputs and outputs without any side effects.  In the case of automating notebooks, coding notebooks in this way helps reduce any unintended side effects where each stage in a pipeline can operate independently from the rest.
# MAGIC 
# MAGIC More complex workflows using notebooks require managing dependencies between tasks and passing parameters into notebooks.  Dependency management can done by chaining notebooks together, for instance to run reporting logic after having completed a database write. Sometimes, when these pipelines become especially complex, chaining notebooks together isn't sufficient. In those cases, scheduling with Apache Airflow has become the preferred solution. Notebook widgets can be used to pass parameters to a notebook when the parameters are determined at runtime.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ETL-Part-3/notebook-workflows.png" style="height: 400px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Widgets
# MAGIC 
# MAGIC Widgets allow for the customization of notebooks without editing the code itself.  They also allow for passing parameters into notebooks.  There are 4 types of widgets:
# MAGIC 
# MAGIC | Type          | Description                                                                                        |
# MAGIC |:--------------|:---------------------------------------------------------------------------------------------------|
# MAGIC | `text`        | Input a value in a text box.                                                                       |
# MAGIC | `dropdown`    | Select a value from a list of provided values.                                                     |
# MAGIC | `combobox`    | Combination of text and dropdown. Select a value from a provided list or input one in the text box.|
# MAGIC | `multiselect` | Select one or more values from a list of provided values.                                          |
# MAGIC 
# MAGIC Widgets are Databricks utility functions that can be accessed using the `dbutils.widgets` package and take a name, default value, and values (if not a `text` widget).
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://docs.azuredatabricks.net/user-guide/notebooks/widgets.html#id1" target="_blank">the Databricks documentation on widgets for additional information </a>

# COMMAND ----------

dbutils.widgets.dropdown("MyWidget", "1", [str(x) for x in range(1, 5)])

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the widget created at the top of the screen.  Choose a number from the dropdown menu.  Now, bring that value into your code using the `get` method.

# COMMAND ----------

dbutils.widgets.get("MyWidget")

# COMMAND ----------

# MAGIC %md
# MAGIC Clear the widgets using either `remove()` or `removeAll()`

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC While great for adding parameters to notebooks and dashboards, widgets also allow us to pass parameters into notebooks when we run them like a Python or JAR file.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Running Notebooks
# MAGIC 
# MAGIC There are two options for running notebooks.  The first is using `dbutils.notebook.run("<path>", "<timeout>")`.  This will run the notebook.  [Take a look at this notebook first to see what it accomplishes.]($./Runnable/Runnable-1 )
# MAGIC 
# MAGIC Now run the notebook with the following command.

# COMMAND ----------

return_value = dbutils.notebook.run("./Runnable/Runnable-1", 30)

print("Notebook successfully ran with return value: {}".format(return_value))

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how the `Runnable-1` notebook ends with the command `dbutils.notebook.exit("returnValue")`.  This is a `string` that's passed back into the running notebook's environment.  Run the following cell to import the variable defined within `Runnable-1`

# COMMAND ----------

try:
  print(my_variable)
except NameError:
  print("Variable not defined")

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC This variable is not passed into our current environment.  The difference between `dbutils.notebook.run()` and `%run` is that the parent notebook will inherit variables from the ran notebook with `%run`.

# COMMAND ----------

# MAGIC %run ./Runnable/Runnable-1

# COMMAND ----------

# MAGIC %md
# MAGIC Now this variable is available for use in this notebook

# COMMAND ----------

print(my_variable)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameter Passing and Debugging
# MAGIC 
# MAGIC Notebook widgets allow us to pass parameters into notebooks.  This can be done in the form of a dictionary that maps the widget name to a value as a `string`.
# MAGIC 
# MAGIC [Take a look at the second notebook to see what it accomplishes.]($./Runnable/Runnable-2 )

# COMMAND ----------

# MAGIC %md
# MAGIC Pass your parameters into `dbutils.notebook.run` and save the resulting return value

# COMMAND ----------

result = dbutils.notebook.run("./Runnable/Runnable-2", 60, {"date": "11-27-2013", "dest_path": "/tmp/dest_path"})

# COMMAND ----------

# MAGIC %md
# MAGIC Click on `Notebook job #XXX` above to view the output of the notebook.  **This is helpful for debugging any problems.**

# COMMAND ----------

# MAGIC %md
# MAGIC Parse the JSON string results

# COMMAND ----------

import json
print(json.loads(result))

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Dependency Management, Timeouts, Retries, and Passing Data
# MAGIC 
# MAGIC Running notebooks can allow for more advanced workflows in the following ways:<br><br>
# MAGIC 
# MAGIC * Managing **dependencies** can be ensured by running a notebook that triggers other notebooks in the desired order
# MAGIC * Setting **timeouts** ensures that jobs have a set limit on when they must either complete or fail
# MAGIC * **Retry logic** ensures that fleeting failures do not prevent the proper execution of a notebook
# MAGIC * **Data can passed** between notebooks by saving the data to a blob store or table and passing the path as an exit parameter
# MAGIC 
# MAGIC <img alt="Side Note" title="Side Note" style="vertical-align: text-bottom; position: relative; height:1.75em; top:0.05em; transform:rotate(15deg)" src="https://files.training.databricks.com/static/images/icon-note.webp"/> Check out <a href="https://docs.azuredatabricks.net/user-guide/notebooks/notebook-workflows.html" target="_blank">the Databricks documentation on Notebook Workflows for additional information </a>

# COMMAND ----------

# MAGIC %md
# MAGIC ### An example of a simple ETL job
# MAGIC 
# MAGIC 1. Takes four parameters: 
# MAGIC   - Azure Storage account
# MAGIC   - Azure Blob Storage container with CSV files
# MAGIC   - SAS key
# MAGIC   - Output pathname
# MAGIC 1. Reads the CSV files to DataFrame and infers schema
# MAGIC 1. Renames the columns so there and no spaces or invalid characters.
# MAGIC 1. Writes the result to DBFS as a parquet file 
# MAGIC 1. Exits with the input path and output path as a result

# COMMAND ----------

SAS_KEY = "?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D"
STORAGE_ACCOUNT = "azureailabs"
CONTAINER = "chicago-crime"
OUTPUT_PATH = "/datasets/chicagocrimes.parquet"

dbutils.notebook.run("./Runnable/load_parquet", 60, 
                     {"SAS_KEY": SAS_KEY,
                      "STORAGE_ACCOUNT": STORAGE_ACCOUNT,
                      "CONTAINER": CONTAINER,
                      "OUTPUT_PATH": OUTPUT_PATH })

# COMMAND ----------

crimeDF = spark.read.parquet(OUTPUT_PATH)
display(crimeDF)

# COMMAND ----------

