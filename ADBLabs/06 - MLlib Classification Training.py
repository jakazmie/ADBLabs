# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Developing machine learning classification model with Spark MLlib
# MAGIC 
# MAGIC In this lab you will learn how to develop a machine learning classification model using Spark MLlib in Azure Databricks environment. During the course of the lab you will walk through cardinal phases of a machine learning workflow from data gathering and cleaning through feature engineering and modeling to model inferencing.
# MAGIC 
# MAGIC ## Lab scenario
# MAGIC You will develop a machine learning classification model to predict customer churn. The dataset used during the lab contains historical information about customers of a fictional telecomunication company. You will use Azure Databricks unified analytics platform and Spark MLlib library to implement the ML workflow resulting in a customer churn prediction model.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## MLlib Overview
# MAGIC 
# MAGIC ### What is MLlib?
# MAGIC 
# MAGIC MLlib is a package, built on and included in Spark, that provides interfaces for
# MAGIC - gathering and cleaning data,
# MAGIC - feature engineering and feature selection,
# MAGIC - training and tuning large scale supervised and unsupervised machine learning models, 
# MAGIC - and using those models in production.
# MAGIC 
# MAGIC ### MLlib Concepts
# MAGIC 
# MAGIC ![MLlib](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/MLlib.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 
# MAGIC ## Gather, Analyze, and Preprocess data
# MAGIC 
# MAGIC ### Load and review data
# MAGIC 
# MAGIC We begin by loading and doing a rudimentary analysis of customer churn historical data, which is stored in CSV format in Azure Blob.  

# COMMAND ----------

# Reset the widgets
dbutils.widgets.removeAll()

# COMMAND ----------

# Set up notebook parameters
dbutils.widgets.text("STORAGE_ACCOUNT", "azureailabs")
dbutils.widgets.text("CONTAINER", "churn")
dbutils.widgets.text("ACCOUNT_KEY", "")

# COMMAND ----------

# Load data from Azure Blob
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT").strip()
CONTAINER = dbutils.widgets.get("CONTAINER").strip()
ACCOUNT_KEY = dbutils.widgets.get("ACCOUNT_KEY").strip()

if ACCOUNT_KEY != "":
  # Set up account access key
  conf_key = "fs.azure.account.key.{storage_acct}.blob.core.windows.net".format(storage_acct=STORAGE_ACCOUNT)
  spark.conf.set(conf_key, ACCOUNT_KEY)

source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
  
# Read the data from the default datasets repository in Databricks
df = spark.read.option("header", True).option("inferSchema", True).csv(source_str)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The `Churn` column indicates whether the customer changed providers. This is our `target` variable or `label`. The goal of our model is to predict this column on new examples.
# MAGIC 
# MAGIC The subset of other columns will be used as predictors or features.
# MAGIC 
# MAGIC Some of the columns - e.g. `customerid` and `callingnum` - are not good candidates for features. They don't capture much information about the customer profile and may *leak the target* in the model. We will remove them from the training dataset.
# MAGIC 
# MAGIC There also some suspicious records. The first two records indicate that a 12 year old makes over $160,000 a year. Although it is possible it is highly improbable.
# MAGIC 
# MAGIC Let's drill down a little bit.

# COMMAND ----------

from pyspark.sql.functions import when

display(
  df.withColumn("agegroup", 
                when(df.age<=13, '0-13')
                .when((df.age>13) & (df.age<18), '14-16')
                .otherwise('>17'))
  .withColumn("incomegroup",
                  when(df.annualincome==0, 'O')
                  .when((df.annualincome>0) & (df.annualincome<10000) , '<10K')
                  .otherwise('>10K'))
  .groupBy('agegroup', 'incomegroup').count()
)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC There 583 records of young kids making mor than $10,000. For the sake of this lab we will assume that these are errorneous records and remove them.
# MAGIC 
# MAGIC We will now create a DataFrame with an explicitly defined schema. We will also remove irrelevant columns and suspicious rows.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Cleanse data

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
  StructField("age", DoubleType()),
  StructField("annualincome", DoubleType()),
  StructField("calldroprate", DoubleType()),
  StructField("callfailurerate", DoubleType()),
  StructField("callingnum", StringType()),
  StructField("customerid", StringType()),
  StructField("customersuspended",  StringType()),
  StructField("education",  StringType()),
  StructField("gender", StringType()),
  StructField("homeowner", StringType()),
  StructField("maritalstatus", StringType()),
  StructField("monthlybilledamount", DoubleType()),
  StructField("noadditionallines", StringType()),
  StructField("numberofcomplaints", DoubleType()),
  StructField("numberofmonthunpaid", DoubleType()),
  StructField("numdayscontractequipmentplanexpiring", DoubleType()),
  StructField("occupation", StringType()),
  StructField("penaltytoswitch", DoubleType()),
  StructField("state", StringType()),
  StructField("totalminsusedinlastmonth", DoubleType()),
  StructField("unpaidbalance", DoubleType()),
  StructField("usesinternetservice", StringType()),
  StructField("usesvoiceservice", StringType()),
  StructField("percentagecalloutsidenetwork", DoubleType()),
  StructField("totalcallduration", DoubleType()),
  StructField("avgcallduration", DoubleType()),
  StructField("churn", DoubleType()),
  StructField("year", DoubleType()),
  StructField("month", DoubleType())
])

df = (spark.read
     .option("header", True)
     .schema(schema)
     .csv(source_str))

display(df)

# COMMAND ----------

clean_df = (df.drop("customerid", "callingnum", "year", "month")
    .dropDuplicates()
    .filter(~ ((df.age<14) & (df.annualincome>10000))))
  
display(clean_df.groupBy("churn").count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC As shown by the last query, our dataset is unbalanced with respect to the class label. We will have to take it under consideration when training the model.

# COMMAND ----------

# MAGIC %md #### Split data into training and test sets
# MAGIC 
# MAGIC At this point we will split our dataset into separate training and test sets. 

# COMMAND ----------

# Split the dataset randomly into 85% for training and 15% for testing.

train, test = clean_df.randomSplit([0.85, 0.15], 0)
print("We have {} training examples and {} test examples.".format(train.count(), test.count()))

# COMMAND ----------

# MAGIC %md #### Visualize our data
# MAGIC 
# MAGIC Now that we have preprocessed our features and prepared a training dataset, we can use visualizations to get more insights about the data.
# MAGIC 
# MAGIC Calling `display()` on a DataFrame in Databricks and clicking the plot icon below the table will let you draw and pivot various plots.  See the [Visualizations section of the Databricks Guide](https://docs.databricks.com/user-guide/visualizations/index.html) for more ideas.

# COMMAND ----------

display(train)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can also use other visualization libraries.

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

# Get a sample of data
sample = train.sample(False, 0.05, 42).toPandas()

ax = sample.plot.scatter(x='percentagecalloutsidenetwork', y='avgcallduration')
display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Save training and testing data
# MAGIC 
# MAGIC At this point, we are going to save the datasets using `Parquet` format

# COMMAND ----------

test.write.mode("overwrite").parquet("/datasets/churn_test_data")
train.write.mode("overwrite").parquet("/datasets/churn_train_data")

# COMMAND ----------

# MAGIC 
# MAGIC %md ### Train a Machine Learning Pipeline
# MAGIC 
# MAGIC Now that we have understood our data and prepared it as a DataFrame with pre-processed data, we are ready to train an ML classifier. In this lab we will focus on a single algorithm - Gradient-boosted tree classifier - however in most cases you should go through a more thorough model selection process to find an algorithm that best fits you scenario and training data. We will also demonstrate how to automate hyperparameter tuning using Spark ML validators.
# MAGIC 
# MAGIC To achieve it, we will put together a simple Spark ML Pipeline.
# MAGIC 
# MAGIC Most Spark ML algorithms, including GBT, expect the training data to be provided as a *numeric* column to represent the label and a column of type *Vector* to represent the features. 
# MAGIC 
# MAGIC The features in our datasets are a mix of *numeric* and *string* values. *String* columns represent categorical features. Most *numeric* columns are continous features. Before we can configure hyper parameter tuning and Random Forest stages of our pipeline we will need to add a few data transformation steps.
# MAGIC 
# MAGIC Our complete pipeline has the following stages:
# MAGIC 
# MAGIC * `StringIndexer`: Convert string columns to categorical features
# MAGIC * `VectorAssembler`: Assemble the feature columns into a feature vector.
# MAGIC * `VectorIndexer`: Identify columns which should be treated as categorical. This is done heuristically, identifying any column with a small number of distinct values as being categorical.  For us, this will include columns like `occupation` or `homeowner` .
# MAGIC * `Classifier`: This stage will train the classification algorithm.
# MAGIC * `CrossValidator`: The machine learning algorithms have several [hyperparameters](https://en.wikipedia.org/wiki/Hyperparameter_optimization), and tuning them to our data can improve performance of the model.  We will do this tuning using Spark's [Cross Validation](https://en.wikipedia.org/wiki/Cross-validation_&#40;statistics&#41;) framework, which automatically tests a grid of hyperparameters and chooses the best.
# MAGIC 
# MAGIC ![Image of Pipeline](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/pipeline.png)

# COMMAND ----------

# MAGIC %md First, we define the feature processing stages of the Pipeline:
# MAGIC * Convert string columns to categorical features. 
# MAGIC * Assemble feature columns into a feature vector. 
# MAGIC * Identify categorical features, and index them.
# MAGIC ![Image of feature processing](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/features.png)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, VectorAssembler, VectorIndexer
from pyspark.sql.types import *
from pyspark.ml import Pipeline

# Create a list of string indexers - one for each string column
stringCols = [field.name for field in train.schema if field.dataType == StringType()]
stringIndexers = [StringIndexer().setInputCol(name).setOutputCol(name+"_idx") for name in stringCols]

# Get a list of all numeric columns
numericCols = [field.name for field in train.schema if field.dataType != StringType()]

# Remove a label column
numericCols.remove('churn')

# Create a list of all feature columns
featureCols = numericCols + [name + "_idx" for name in stringCols]

# This concatenates all feature columns into a single feature vector in a new column "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featureCols, outputCol="rawFeatures")

# This identifies categorical features and indexes them.
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

# Create a pipeline
stages = stringIndexers + [vectorAssembler, vectorIndexer]
pipeline = Pipeline(stages=stages)

# Check the Pipeline operation
display(pipeline.fit(train).transform(train))


# COMMAND ----------

# MAGIC %md Second, we define the model training stage of the Pipeline. `GBTClassifier` takes feature vectors and labels as input and learns to predict labels of new examples.
# MAGIC ![RF image](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/train.png)

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
# Takes the "features" column and learns to predict "churn"
classifier = GBTClassifier(labelCol="churn", featuresCol="features", maxBins=50)

# COMMAND ----------

# MAGIC %md Third, we wrap the model training stage within a `CrossValidator` stage.  `CrossValidator` knows how to call the classifier algorithm with different hyperparameter settings.  It will train multiple models and choose the best one, based on minimizing some metric.  In this lab, our metric is *AUC*.
# MAGIC 
# MAGIC ![Crossvalidate](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/tune.png)

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
# Define a grid of hyperparameters to test:
#  - maxDepth: max depth of each decision tree in the GBT ensemble
#  - maxIter: iterations, i.e., number of trees in each GBT ensemble
# In this example notebook, we keep these values small.  In practice, to get the highest accuracy, you would likely want to try deeper trees (10 or higher) and more trees in the ensemble (>100).
paramGrid = ParamGridBuilder()\
  .addGrid(classifier.maxDepth, [5, 7])\
  .addGrid(classifier.maxIter, [10, 50])\
  .build()
# Define a binary evaluator
evaluator = BinaryClassificationEvaluator(labelCol=classifier.getLabelCol(), rawPredictionCol=classifier.getPredictionCol())
# Declare the CrossValidator, which runs model tuning for us.
cv = CrossValidator(estimator=classifier, evaluator=evaluator, estimatorParamMaps=paramGrid, numFolds=3)

# COMMAND ----------

# MAGIC %md Finally, we can tie our feature processing and model training stages together into a single `Pipeline`.
# MAGIC 
# MAGIC ![Image of Pipeline](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/pipeline.png)

# COMMAND ----------

from pyspark.ml import Pipeline

stages = pipeline.getStages()
stages = stages + [cv]
pipeline.setStages(stages)
print(pipeline.getStages())

# COMMAND ----------

# MAGIC %md #### Train the Pipeline!
# MAGIC 
# MAGIC Now that we have set up our workflow, we can train the Pipeline in a single call.  Calling `fit()` will run feature processing, model tuning, and training in a single call.  We get back a fitted Pipeline with the best model found.
# MAGIC 
# MAGIC ***Note***: This next cell can take up to **10 minutes**.  This is because it is training *a lot* of trees:
# MAGIC * For each random sample of data in Cross Validation,
# MAGIC   * For each setting of the hyperparameters,
# MAGIC     * `CrossValidator` is training a separate GBT ensemble which contains many Decision Trees.
# MAGIC     
# MAGIC Since our training set is unbalanced we will apply a technique called *under sampling*. We will use all instances of a minority class but select a random sample from the majority class. 

# COMMAND ----------

# Load training data
train = spark.read.parquet("/datasets/churn_train_data")

# Undersample majority class
stratified_train = train.sampleBy('Churn', fractions={0: 0.2, 1: 1.0}).cache()

display(stratified_train.groupby('Churn').count())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Start training.

# COMMAND ----------

pipelineModel = pipeline.fit(stratified_train)

# COMMAND ----------

# MAGIC %md ## Make predictions, and evaluate results
# MAGIC 
# MAGIC Our final step will be to use our fitted model to make predictions on new data.  We will use our held-out test set, but you could also use this model to make predictions on completely new data.  
# MAGIC 
# MAGIC We will also evaluate our predictions.  Computing evaluation metrics is important for understanding the quality of predictions, as well as for comparing models and tuning parameters.

# COMMAND ----------

# MAGIC %md Calling `transform()` on a new dataset passes that data through feature processing and uses the fitted model to make predictions.  We get back a DataFrame with a new column `predictions` (as well as intermediate results such as our `rawFeatures` column from feature processing).

# COMMAND ----------

test = spark.read.parquet("/datasets/churn_train_data")

predictions = pipelineModel.transform(test).cache()

# COMMAND ----------

# MAGIC %md It is easier to view the results when we limit the columns displayed to:
# MAGIC * `churn`: the true churn indicator
# MAGIC * `prediction`: our predicted churn
# MAGIC * feature columns: our original (human-readable) feature columns

# COMMAND ----------

display(predictions.select("churn", "prediction"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Calculate classification performance metrics.
# MAGIC 
# MAGIC ![Confusion matrix](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/confusion.png)
# MAGIC 
# MAGIC The metrics we tried to optimize was *AUC* of ROC.
# MAGIC 
# MAGIC ![ROC](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/roc.png)

# COMMAND ----------

# Calculate AOC
print("{} on our test set: {}".format(evaluator.getMetricName(), evaluator.evaluate(predictions, {})))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Persist the model
# MAGIC 
# MAGIC Spark MLlib supports model persistence. Key features of ML persistence include:
# MAGIC - Support of all language APIs in Spark: Scala, Java, Python & R
# MAGIC - Support for single models and full Pipelines, both unfitted (a "recipe") and fitted (a result)
# MAGIC - Distributed storage using and exchangealbe format
# MAGIC 
# MAGIC In Azure Databricks, by default, the model is saved to and loaded from DBFS.

# COMMAND ----------

model_path = '/models/churn_classifier'
pipelineModel.write().overwrite().save(model_path)

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/models/churn_classifier'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You will use the saved model during the deployment lab.