# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### RDDs, DataFrames, and SQL
# MAGIC 
# MAGIC The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection of elements partitioned across the nodes of the cluster that can be operated on in parallel.:
# MAGIC 
# MAGIC * **Resilient**: They are fault tolerant, so if part of your operation fails, Spark  quickly recovers the lost computation.
# MAGIC * **Distributed**: RDDs are distributed across networked machines known as a cluster.
# MAGIC 
# MAGIC RDDs support two types of operations: transformations, which create a new dataset from an existing one, and actions, which return a value to the driver program after running a computation on the dataset. For example, map is a transformation that passes each dataset element through a function and returns a new RDD representing the results. On the other hand, reduce is an action that aggregates all the elements of the RDD using some function and returns the final result to the driver program (although there is also a parallel reduceByKey that returns a distributed dataset).
# MAGIC 
# MAGIC All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently. For example, we can realize that a dataset created through map will be used in a reduce and return only the result of the reduce to the driver, rather than the larger mapped dataset.
# MAGIC 
# MAGIC By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also persist an RDD in memory using the persist (or cache) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.
# MAGIC 
# MAGIC **RDD** is the Spark's low level distributed collection API. RDDs are at the foundation of higher level APIs - DataFrames and Datasets. DataFrames and Datasets are the recommended APIs for most data engineering tasks. Both DataFrames and Datasets APIs are the interface to the Sparks module called Spark SQL. Unlike the basic Spark RDD API, the interfaces provided by Spark SQL provide Spark with more information about the structure of both the data and the computation being performed. Internally, Spark SQL uses this extra information to perform extra optimizations.
# MAGIC 
# MAGIC A Dataset is a distributed collection of data. Dataset is a new interface added in Spark 1.6 that provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine. A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.). The Dataset API is available in Scala and Java. Python does not have the support for the Dataset API. But due to Python’s dynamic nature, many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally row.columnName). The case for R is similar.
# MAGIC 
# MAGIC A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs. The DataFrame API is available in Scala, Java, Python, and R. In Scala and Java, a DataFrame is represented by a Dataset of Rows. In the Scala API, DataFrame is simply a type alias of Dataset[Row]. While, in Java API, users need to use Dataset<Row> to represent a DataFrame.
# MAGIC   
# MAGIC **DataFrames** will be the primary API used in this workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Creating DataFrames
# MAGIC 
# MAGIC A DataFrame can be created from a multitude of data sources, including:
# MAGIC 
# MAGIC - Azure Blob Storage
# MAGIC - Azure Data Lake Storage Gen1
# MAGIC - Azure Data Lake Storage Gen2
# MAGIC - Azure Cosmos DB
# MAGIC - Azure SQL Data Warehouse
# MAGIC - CSV 
# MAGIC - Parquet 
# MAGIC - Avro
# MAGIC - JSON
# MAGIC - LZO compressed files
# MAGIC - Zip files
# MAGIC 
# MAGIC The current full list of supported data sources can be found <a href=https://docs.azuredatabricks.net/spark/latest/data-sources/index.html> here </a>
# MAGIC 
# MAGIC 
# MAGIC When loading data from the files, the files can retrieved directly from Azure Blob Sotorage or Azure Data Lake or can be  be uploaded to Azure Databricks' distributed file system DBFS that is by default mounted on all Databricks clusters in the Workspace. Files in DBFS persist to Azure Blob storage and are available even after a cluster was terminated. 
# MAGIC 
# MAGIC #### Create a data frame Azure SQL Database
# MAGIC 
# MAGIC You can use Azure Databricks to query Microsoft SQL Server and Azure SQL Database tables using the JDBC drivers that come with Databricks Runtime 3.4 and above.

# COMMAND ----------

jdbcHostname = "jksqldbserver.database.windows.net"
jdbcDatabase = "jksqldb"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)

jdbcUserName = "demouser"
jdbcPassword = "IrvineLab2018"

connectionProperties = {
  "user" : jdbcUserName,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# pushdown_query = "(select * from SalesLT.Customer where CompanyName='Progressive Sports') emp_alias"
df = spark.read.jdbc(url=jdbcUrl, table="SalesLT.Customer", properties=connectionProperties)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Create a DataFrame from files in Azure Blob Storage
# MAGIC 
# MAGIC There are two ways of accessing files stored in Azure Blob Storage:
# MAGIC - You can mount a Blob Storage container or a folder inside a container on Databricks File System. The mount is a pointer to a Blob Storage container, so the data is never synced locally
# MAGIC - You can access Azure Blob Storage directly using the DataFrame API.
# MAGIC 
# MAGIC ##### Mount Azure Blob Storage containers with DBFS
# MAGIC 
# MAGIC To mount a Blob Storage container or a folder inside a container, use the following command: 

# COMMAND ----------

STORAGE_ACCOUNT = "azureailabs"
CONTAINER = "people10m"
MOUNT_POINT = "/mnt/people10m"
SAS_KEY = "?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D"

source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
conf_key = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

dbutils.fs.mount(
    source = source_str,
    mount_point = MOUNT_POINT,
    extra_configs = {conf_key: SAS_KEY} 
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can now explore the mount folder using `%fs ls` command.

# COMMAND ----------

# MAGIC %fs ls /mnt/people10m

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC You can use Databricks file system commands to work with the files in Azure Blob Storage. For example you can display the first few lines of a file using the `head` command.

# COMMAND ----------

# MAGIC %fs head mnt/people10m/part-00000-tid-4686375192325307263-49bc1ac5-354f-421d-8bc5-8877a0140ecc-610-c000.csv

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To create a DataFrame use `spark.read` referring to remote files like they were located in the cluster's local file system.

# COMMAND ----------

peopleDF = spark.read.csv("/mnt/people10m")
display(peopleDF)

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(magrittr)
# MAGIC 
# MAGIC peopleDF <- read.df("/mnt/people10m", source="csv")
# MAGIC display(peopleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Note that the above command did not properly parsed the header row and it did not discover column data types. We will address it in the later steps.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To unmount the remote blob storage execute:

# COMMAND ----------

dbutils.fs.unmount(MOUNT_POINT)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Access Azure Blob Storage directly 
# MAGIC 
# MAGIC You can also read data from Azure Blob Storage using the Spark API.

# COMMAND ----------

# Set up a SAS for a container
spark.conf.set(conf_key, SAS_KEY)

# COMMAND ----------

# Create a dataframe parsing the header and infering the schema
peopleDF = spark.read.option("header", True).option("inferSchema", True).csv(source_str)
    
display(peopleDF)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC source_str <- "wasbs://people10m@azureailabs.blob.core.windows.net/"
# MAGIC peopleDF <- read.df(source_str, source="csv", header="true", inferSchema="true")
# MAGIC display(peopleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC This time we specified additional options of `spark.read` to parse the header and infer the schema.
# MAGIC 
# MAGIC You can also specify the schema explicitly.

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField("id", IntegerType()),
  StructField("firstName", StringType()),
  StructField("middleName", StringType()),
  StructField("lastName", StringType()),
  StructField("gender", StringType()),
  StructField("birthDate", TimestampType()),
  StructField("ssn",  StringType()),
  StructField("salary", IntegerType())
])

# Create a dataframe parsing the header and applying schema
peopleDF = (spark.read
            .option("header", True)
            .schema(schema)
            .csv(source_str))

display(peopleDF)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC schema <- structType(
# MAGIC   structField("id", "integer"),
# MAGIC   structField("firstName", "string"),
# MAGIC   structField("middleName", "string"),
# MAGIC   structField("lastName", "string"),
# MAGIC   structField("gender", "string"),
# MAGIC   structField("birthDate", "timestamp"),
# MAGIC   structField("ssn",  "string"),
# MAGIC   structField("salary", "integer")
# MAGIC )
# MAGIC 
# MAGIC source_str <- "wasbs://people10m@azureailabs.blob.core.windows.net/"
# MAGIC peopleDF <- read.df(source_str, source="csv", header="true", schema=schema)
# MAGIC display(peopleDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Saving DataFrames
# MAGIC 
# MAGIC After processing a DataFrame can be written back to any of the supported data sources. When saving the DataFrame to a file the recommended file format is `Parquet`. [Apache Parquet](https://parquet.apache.org/documentation/latest/) is a highly efficient, column-oriented data format that shows massive performance increases over other options such as CSV. In addition to a very efficient compression, Parquet  preserves the schema when serializing a DataFrame.
# MAGIC 
# MAGIC When writing data to DBFS, the best practice is to use Parquet.
# MAGIC 
# MAGIC ##### Save the `peopleDF` (including infered schema) to `Parquet`

# COMMAND ----------

peopleDF.write.mode("overwrite").parquet("/people10m.parquet")

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC write.parquet(x=peopleDF, path="/people10m.parquet", mode="overwrite")

# COMMAND ----------

# MAGIC %fs ls dbfs:/people10m.parquet

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interacting with DataFrames
# MAGIC 
# MAGIC Once created (instantiated), a DataFrame object has methods attached to it. Methods are operations one can perform on DataFrames such as filtering, counting, aggregating and many others.
# MAGIC 
# MAGIC In working with DataFrames, it is common to chain operations together, such as: `df.select().filter().orderBy()`.  
# MAGIC 
# MAGIC By chaining operations together, you don't need to save intermediate DataFrames into local variables (thereby avoiding the creation of extra objects).
# MAGIC 
# MAGIC Also note that you do not have to worry about how to order operations because the optimizier determines the optimal order of execution of the operations for you.
# MAGIC 
# MAGIC `df.select(...).orderBy(...).filter(...)`
# MAGIC 
# MAGIC versus
# MAGIC 
# MAGIC `df.filter(...).select(...).orderBy(...)`
# MAGIC 
# MAGIC As noted in the introduction section Spark allows two distinct kinds of operations: 
# MAGIC - transformations, and
# MAGIC - actions
# MAGIC 
# MAGIC ##### Transformations
# MAGIC 
# MAGIC Transformations are operations that will not be completed at the time you write and execute the code in a cell - they will only get executed once you have called a **action**. An example of a transformation might be to convert an integer into a float or to filter a set of values.
# MAGIC 
# MAGIC ##### Actions
# MAGIC 
# MAGIC Actions are commands that are computed by Spark right at the time of their execution. They consist of running all of the previous transformations in order to get back an actual result. An action is composed of one or more jobs which consists of tasks that will be executed by the workers in parallel where possible
# MAGIC 
# MAGIC Here are some simple examples of transformations and actions. Remember, these **are not all** the transformations and actions - this is just a short sample of them. 
# MAGIC 
# MAGIC ![transformations and actions](https://training.databricks.com/databricks_guide/gentle_introduction/trans_and_actions.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To continue, load the data you saved to DBFS in the previous step back into the dataframe. Note that you don't need to specify any explicit information about the schema as the schema metadata was saved with the `parquet` file.

# COMMAND ----------

peopleDF = spark.read.parquet("/people10m.parquet")
peopleDF.printSchema()

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC peopleDF <- read.parquet("/people10m.parquet")
# MAGIC printSchema(peopleDF)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Querying Data
# MAGIC 
# MAGIC You can use the DataFrame object's methods to query the data. For example to find out which women were born after 1990 use `select` and `filter` methods.

# COMMAND ----------

from pyspark.sql.functions import year
from pyspark.sql.functions import col

display(
  peopleDF 
    .select("firstName","middleName","lastName","birthDate","gender") 
    .filter(col("gender") == 'F') 
    .filter(year("birthDate") > "1990")
)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC peopleDF %>% 
# MAGIC filter(peopleDF$gender == "F") %>%
# MAGIC filter(year(peopleDF$birthDate) > "1990") %>%
# MAGIC select("firstName","middleName","lastName","birthDate","gender") %>%
# MAGIC display

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can create derived columns in your queries. The following statement finds women born after 1990; it uses the `year` to create a `birthYear` column on the fly.

# COMMAND ----------

display(
  peopleDF.select("firstName","middleName","lastName",year("birthDate").alias('birthYear'),"salary") 
    .filter(year("birthDate") > "1990") 
    .filter("gender = 'F' ")
)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC peopleDF %>% 
# MAGIC filter(peopleDF$gender == "F") %>%
# MAGIC filter(year(peopleDF$birthDate) > "1990") %>%
# MAGIC mutate(birthYear=year(peopleDF$birthDate)) %>%
# MAGIC select("firstName","middleName","lastName","birthYear","gender") %>%
# MAGIC display

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Visualizations
# MAGIC 
# MAGIC 
# MAGIC Databricks provides easy-to-use, built-in visualizations for your data. 
# MAGIC 
# MAGIC Display the data by invoking the Spark `display` function.
# MAGIC 
# MAGIC Visualize the results by selecting one of the graphs available once the table is displayed:
# MAGIC 
# MAGIC <img src="https://files.training.databricks.com/images/eLearning/visualization-1.png" style="border: 1px solid #aaa; padding: 10px; border-radius: 10px 10px 10px 10px"/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the next steps we will visualize the anwser to the question: How many women were named Mary in each year?

# COMMAND ----------

marysDF = (peopleDF.select(year("birthDate").alias("birthYear")) 
  .filter("firstName = 'Mary' ") 
  .filter("gender = 'F' ") 
  .orderBy("birthYear") 
  .groupBy("birthYear") 
  .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC %md
# MAGIC To start the visualization process, first apply the `display` function to the DataFrame. 
# MAGIC 
# MAGIC Next, click the graph button in the bottom left corner (second from left) to display data in different ways.

# COMMAND ----------

display(marysDF)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next we will compare popularity of two names from 1990 onwards.

# COMMAND ----------


dordonDF = (peopleDF 
  .select(year("birthDate").alias("birthYear"), "firstName") 
  .filter((col("firstName") == 'Donna') | (col("firstName") == 'Dorothy')) 
  .filter(col("gender") == 'F') 
  .filter(year("birthDate") > 1990) 
  .orderBy("birthYear") 
  .groupBy("birthYear", "firstName") 
  .count()
)

# COMMAND ----------

display(dordonDF)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC dordonDF <- peopleDF %>%
# MAGIC filter(peopleDF$firstName == "Donna" | peopleDF$firstName == 'Dorothy') %>%
# MAGIC filter(peopleDF$gender == "F") %>%
# MAGIC filter(year(peopleDF$birthDate) > 1990) %>%
# MAGIC mutate(birthYear=year(peopleDF$birthDate)) %>%
# MAGIC select("birthYear", "firstName") %>%
# MAGIC orderBy("birthYear") %>%
# MAGIC groupBy("birthYear", "firstName") %>%
# MAGIC count %>%
# MAGIC display

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary Views and SQL
# MAGIC 
# MAGIC In DataFrames, <b>temporary views</b> are used to make the DataFrame available to SQL, and work with SQL syntax directly.
# MAGIC 
# MAGIC A temporary view gives you a name to query from SQL, but unlike a table it exists only for the duration of your Spark Session. As a result, the temporary view will not carry over when you restart the cluster or switch to a new notebook. It also won't show up in the Data button on the menu on the left side of a Databricks notebook which provides easy access to databases and tables.
# MAGIC 
# MAGIC The statement in the following cells create a temporary view containing the the full data set.

# COMMAND ----------

peopleDF.createOrReplaceTempView("People10M")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can now use SQL to query the view.

# COMMAND ----------

display(spark.sql("SELECT * FROM  People10M WHERE firstName = 'Donna' "))

# COMMAND ----------

# MAGIC %md
# MAGIC **Note**: In Databricks, `%sql` is the equivalent of `display()` combined with `spark.sql()`:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM People10M WHERE firstName = 'Donna' 

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can create temporary views with a more specific query.

# COMMAND ----------

womenBornAfter1990DF = (peopleDF 
  .select("firstName", "middleName", "lastName",year("birthDate").alias("birthYear"), "salary") 
  .filter(year("birthDate") > 1990) 
  .filter("gender = 'F' ") 
)

# COMMAND ----------

womenBornAfter1990DF.createOrReplaceTempView("womenBornAfter1990")

# COMMAND ----------

# MAGIC %md
# MAGIC Once a temporary view has been created, it can be queried as if it were a table. 
# MAGIC 
# MAGIC Find out how many Marys are in the WomenBornAfter1990 DataFrame.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM womenBornAfter1990 where firstName = 'Mary' 

# COMMAND ----------

