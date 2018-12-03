# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md # Machine Learning with Spark MLlib
# MAGIC 
# MAGIC This Python notebook demonstrates how to develop machine learning models with Spark MLlib.
# MAGIC 
# MAGIC MLlib is a package, built on and included in Spark, that provides interfaces for
# MAGIC - gathering and cleaning data,
# MAGIC - feature engineering and feature selection,
# MAGIC - training and tuning large scale supervised and unsupervised machine learning models, 
# MAGIC - and using those models in production.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://s3-us-west-2.amazonaws.com/curriculum-release/images/105/logo_spark_tiny.png) MLlib Concepts
# MAGIC 
# MAGIC ![MLlib](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/MLlib.png)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Classification for customer churn prediction
# MAGIC 
# MAGIC In this notebook we build on the previous lab and develop a machine learning model to predict customer churn.
# MAGIC 
# MAGIC ### Analyze and Preprocess data
# MAGIC 
# MAGIC #### Load and review data
# MAGIC 
# MAGIC We begin by loading and doing a rudimentary analysis of customer churn historical data, which is stored in CSV format in Azure Blob.  

# COMMAND ----------

# Set up the widgets
dbutils.widgets.text("STORAGE_ACCOUNT", "azureailabs")
dbutils.widgets.text("CONTAINER", "churn")

# COMMAND ----------

# Load data from Azure Blob
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT").strip()
CONTAINER = dbutils.widgets.get("CONTAINER").strip()
source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
  
# Read the data from the default datasets repository in Databricks
display(spark.read.option("header", True).option("inferSchema", True).csv(source_str))


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The `Churn` column indicates whether the customer changed providers. This is our `target` variable or `label`. The goal of our model is to predict this column on new examples.
# MAGIC 
# MAGIC The subset of other columns will be used as predictors or features.
# MAGIC 
# MAGIC Some of the columns - e.g. `customerid` and `callingnum` - are not good candidates for features. They don't capture much information about the customer profile and may *leak the target* in the model. We will remove them from the training dataset.
# MAGIC 
# MAGIC There also some suspicious records. The first two records indicate that a 12 year old makes over $160,000 a year. Although it is possible - a trust fund kid or a child movie star - it is highly improbable.
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
# MAGIC At this point we will split our dataset into separate training and test sets. Since our dataset is unbalanced we will implement a stratified split.

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

display(train.groupBy("churn").count())

# COMMAND ----------

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
# MAGIC ![Image of Pipeline](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/complete_pipeline.png)

# COMMAND ----------

# MAGIC %md First, we define the feature processing stages of the Pipeline:
# MAGIC * Convert string columns to categorical features. For the sake of demonstration we will only use a subset of string columns.
# MAGIC * Assemble feature columns into a feature vector. 
# MAGIC * Identify categorical features, and index them.
# MAGIC ![Image of feature processing](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/feature_preprocessing.png)

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

# MAGIC %md Second, we define the model training stage of the Pipeline. `RandomForestClassifier` takes feature vectors and labels as input and learns to predict labels of new examples.
# MAGIC ![RF image](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/training.png)

# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
# Takes the "features" column and learns to predict "churn"
classifier = GBTClassifier(labelCol="churn", featuresCol="features")

# COMMAND ----------

# MAGIC %md Third, we wrap the model training stage within a `CrossValidator` stage.  `CrossValidator` knows how to call the classifier algorithm with different hyperparameter settings.  It will train multiple models and choose the best one, based on minimizing some metric.  In this example, our metric is *weightedRecall*.
# MAGIC 
# MAGIC ![Confusion matrix](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/confusion_matrix.png)

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
# MAGIC ![Image of Pipeline](http://training.databricks.com/databricks_guide/5-pipeline.png)

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

# COMMAND ----------

train = spark.read.parquet("/datasets/churn_train_data")

stratified_train = train.sampleBy('Churn', fractions={0: 0.2, 1: 1.0}).cache()

stratified_train.groupby('Churn').count().toPandas()



# COMMAND ----------

pipelineModel = pipeline.fit(stratified_train)

# COMMAND ----------

# MAGIC %md ## Make predictions, and evaluate results
# MAGIC 
# MAGIC Our final step will be to use our fitted model to make predictions on new data.  We will use our held-out test set, but you could also use this model to make predictions on completely new data.  For example, if we created some features data based on weather predictions for the next week, we could predict bike rentals expected during the next week!
# MAGIC 
# MAGIC We will also evaluate our predictions.  Computing evaluation metrics is important for understanding the quality of predictions, as well as for comparing models and tuning parameters.

# COMMAND ----------

# MAGIC %md Calling `transform()` on a new dataset passes that data through feature processing and uses the fitted model to make predictions.  We get back a DataFrame with a new column `predictions` (as well as intermediate results such as our `rawFeatures` column from feature processing).

# COMMAND ----------

test = spark.read.parquet("/datasets/churn_train_data")

predictions = pipelineModel.transform(test).cache()

# COMMAND ----------

display(predictions.select("churn", "prediction").groupBy("prediction").count())

# COMMAND ----------

# MAGIC %md It is easier to view the results when we limit the columns displayed to:
# MAGIC * `churn`: the true churn indicator
# MAGIC * `prediction`: our predicted churn
# MAGIC * feature columns: our original (human-readable) feature columns

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Calculate performance metrics. The model was fine-tune for *Recall* but we will also calculate the model's *accuracy*.

# COMMAND ----------

# Calculate AOC
print("{} on our test set: {}".format(evaluator.getMetricName(), evaluator.evaluate(predictions, {})))


# COMMAND ----------

# MAGIC %md **(2) Visualization**: Plotting predictions vs. features can help us make sure that the model "understands" the input features and is using them properly to make predictions.  Below, we can see that the model predictions are correlated with the hour of the day, just like the true labels were.
# MAGIC 
# MAGIC *Note: For more expert ML usage, check out other Databricks guides on plotting residuals, which compare predictions vs. true labels.*

# COMMAND ----------

display(predictions.select("hr", "prediction"))

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

model_path = '/models/bike_regression'
pipelineModel.write().overwrite().save(model_path)

# COMMAND ----------

# MAGIC %r
# MAGIC 
# MAGIC ml_save(pipeline_model, "/models/bike_regression_r")

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/models/bike_regression'

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You will use the saved model during the deployment lab.

# COMMAND ----------

# MAGIC %md ## Improving our model
# MAGIC 
# MAGIC You are not done yet!  This section describes how to take this notebook and improve the results even more.  Try copying this notebook into your Databricks account and extending it, and see how much you can improve the predictions.
# MAGIC 
# MAGIC There are several ways we could further improve our model:
# MAGIC * **Expert knowledge**: We may not be experts on bike sharing programs, but we know a few things we can use:
# MAGIC   * The count of rentals cannot be negative.  `GBTRegressor` does not know that, but we could threshold the predictions to be `>= 0` post-hoc.
# MAGIC   * The count of rentals is the sum of `registered` and `casual` rentals.  These two counts may have different behavior.  (Frequent cyclists and casual cyclists probably rent bikes for different reasons.)  The best models for this dataset take this into account.  Try training one GBT model for `registered` and one for `casual`, and then add their predictions together to get the full prediction.
# MAGIC * **Better tuning**: To make this notebook run quickly, we only tried a few hyperparameter settings.  To get the most out of our data, we should test more settings.  Start by increasing the number of trees in our GBT model by setting `maxIter=200`; it will take longer to train but can be more accurate.
# MAGIC * **Feature engineering**: We used the basic set of features given to us, but we could potentially improve them.  For example, we may guess that weather is more or less important depending on whether or not it is a workday vs. weekend.  To take advantage of that, we could build a few feature by combining those two base features.  MLlib provides a suite of feature transformers; find out more in the [ML guide](http://spark.apache.org/docs/latest/ml-features.html).
# MAGIC 
# MAGIC *Good luck!*