# Databricks notebook source
# MAGIC %md
# MAGIC # Transfer Learning with Deep Learning Pipelines
# MAGIC 
# MAGIC Deep Learning  Pipelines is a new library in Azure Databricks that provides **high-level APIs** for scalable deep learning in Python with Apache Spark.
# MAGIC 
# MAGIC The library provides easy to use interfaces for:
# MAGIC 
# MAGIC Working with image data:
# MAGIC * **Loading images** natively in Spark DataFrames
# MAGIC * **Transfer learning**, a super quick way to leverage deep learning
# MAGIC * **Distributed hyperparameter tuning** via Spark MLlib Pipelines
# MAGIC * **Applying deep learning models at scale** to images, using your own or known popular models, to make predictions or transform them into features
# MAGIC 
# MAGIC Working with general tensors:
# MAGIC * **Applying deep learning models at scale** to tensors of up to 2 dimensions
# MAGIC 
# MAGIC Deploying Models in SQL:
# MAGIC * **Deploying models as SQL functions** to empower everyone by making deep learning available in SQL
# MAGIC 
# MAGIC In this lab we will focus on **Transfer Learning**.
# MAGIC 
# MAGIC Transfer learning is one of the fastest (code and run-time-wise) ways to start using deep learning. In a summary, transfer learning  is a machine learning technique that allows to reuse knowledge gained while solving one problem to a different but related problem. For example, knowledge gained while learning to recognize cars could apply when trying to recognize trucks. Transfer Learning makes it feasible to train very effective ML models on relatively small training data sets.
# MAGIC 
# MAGIC In this lab, your will use Transfer Learing to train a custom image classification model. You will use a deep neural network pre-trained on a general computer vision domain (*imagenet* dataset) and specialize it to classify the type of land shown in aerial images of 224-meter x 224-meter plots. 
# MAGIC 
# MAGIC Land use classification models can be used to track urbanization, deforestation, loss of wetlands, and other major environmental trends using periodically collected aerial imagery. The images used in this lab are based on imagery from the U.S. National Land Cover Database. U.S. National Land Cover Database defines six primary classes of land use: *Developed*, *Barren*, *Forested*, *Grassland*, *Shrub*, *Cultivated*.  Example images in each land use class are shown here:
# MAGIC 
# MAGIC Developed | Cultivated | Barren
# MAGIC --------- | ------ | ----------
# MAGIC ![Developed](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/developed1.png) | ![Cultivated](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/cultivated1.png) | ![Barren](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/barren1.png)
# MAGIC 
# MAGIC  
# MAGIC Forested | Grassland | Shrub
# MAGIC -------- | --------- | -----
# MAGIC ![Forested](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/forest1.png) | ![Grassland](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/grassland1.png) | ![Shrub](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/shrub1.png)
# MAGIC 
# MAGIC During the lab you will walk through a typical machine learning workflow.
# MAGIC 
# MAGIC ![Workflow](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/MLWorkflow.png)
# MAGIC 
# MAGIC 
# MAGIC ## Lab setup
# MAGIC 
# MAGIC *Deep Learning Pipelines* are a part of Databricks ML runtime. This notebook will only run on a cluster provisioned with ML Runtime 4.1 or higher.
# MAGIC 
# MAGIC Let's start.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect and prepare data 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Azure Storage container
# MAGIC The images used for training and validation of the model can be accessed in Azure Blob Storage container. The dataset contains 6000 labeled images - 1000 images per land class.

# COMMAND ----------

STORAGE_ACCOUNT = "azureailabs"
CONTAINER = "aerialmed"
MOUNT_POINT = "/mnt/aerial"
SAS_KEY = "?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D"

source_str = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)
conf_key = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=STORAGE_ACCOUNT)

dbutils.fs.mount(
    source = source_str,
    mount_point = MOUNT_POINT,
    extra_configs = {conf_key: SAS_KEY} 
)

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/aerial

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Prepare training and validation dataframes
# MAGIC 
# MAGIC Deep Learning Pipelines require training data to be loaded into Spark DataFrames. The below code utilizes Spark's native support for image data to load 6000 training images to a DataFrame. It than adds a new column called `label` that annotates an image with a type of land it depicts. The  label is extracted from the pathname of the image.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load training images to a dataframe

# COMMAND ----------

from pyspark.ml.image import ImageSchema
from pyspark.sql.functions import lit

img_dir = '/mnt/aerial/'

img_df = ImageSchema.readImages(img_dir + 'train', recursive=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Add the label column and split data into training and validation DataFrames.

# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col
from pyspark.ml.feature import StringIndexer

# Add a label columns
img_labeled = img_df.withColumn('label', regexp_extract(col('image.origin'), '(.)(train/)(\w+)', 3))
# Split a dataframe into training and validation dataframes
img_train, img_validate = img_labeled.randomSplit([0.7, 0.3])
# Repartition the data frames to enable better parallelization 
img_train = img_train.repartition(64).cache()
img_validate = img_validate.repartition(64).cache()

display(img_train.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train and evaluate model 
# MAGIC 
# MAGIC As noted in the introduction to the lab, we will use Transfer Learning to train a custom image classifier. The classifier's architecture is depicted on the diagram below.
# MAGIC 
# MAGIC ![Model arch](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/tllr.png)
# MAGIC 
# MAGIC The model's input is a raw 224 x 224 image in RGB format. A single image is represented by a 3-dimensional array or a tensor of rank 3. The image is passed to a pre-trained Deep Neural Network - in our case ResNet50 - that converts a raw image to a vector of features - 2048 to be exact. The DNN was trained on a large corpus of images - 14 milion - from different visual domains. As a result, the returned features can be interpreted as essential characteristics of an input image. On top of the pre-trained network we layer a simple multinomial classifier - logistic regression. During training, we effectively only train the logistic regression classifier. The base pre-trained DNN is not modified.
# MAGIC 
# MAGIC You will utilize Spark ML pipeline for defining a training workflow. The pipeline comprises four stages. In stage 1, a string label will be converted to a numeric one - this is the requirement of Spark ML Logistic Regression classifier. In stage 2, a pretrained ResNet50 DNN will be applied as a featurizer. The third stage is a LogisticRegression model. And finally, in stage 4, a predicted label will be converted back to a string.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepare a training pipeline

# COMMAND ----------

from pyspark.ml import Pipeline
from sparkdl import DeepImageFeaturizer 
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import IndexToString, StringIndexer

# Create a label indexer that will convert string labels to numeric.
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(img_validate)

# Create a featurizer based on a pretrained ResNet50 DNN
featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="ResNet50")

# Create a Logistic Regression model
classifier = LogisticRegression(labelCol="indexedLabel", featuresCol="features", maxIter=500, regParam=0.06, elasticNetParam=0.06)

# Create a converter that will convert numeric labels back to original labels
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain the components into a pipeline
pipeline = Pipeline(stages=[labelIndexer, featurizer, classifier, labelConverter])


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Train the model
# MAGIC 
# MAGIC Note, that getting the best results in machine learning requires experimenting with different values for training parameters, an important step called hyperparameter tuning. When using Logistic Regression two key parameters to tune are L1 and L2 regularization parameters. Since Deep Learning Pipelines enables exposing deep learning training as a step in Spark’s machine learning pipelines, users can rely on the hyperparameter tuning infrastructure already built into Spark MLlib. 
# MAGIC 
# MAGIC For the sake of the time, we will skip the hyperparameter tuning process.

# COMMAND ----------

model = pipeline.fit(img_train)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Evaluate the model
# MAGIC 
# MAGIC Now when the model is trained, you should evaluate its performance. Since this is a classification model you can use one or more of classification performance metrics, for example the model's `accuracy`.

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Apply the model to the validation dataset
validated_df = model.transform(img_validate)
# Calculate accuracy
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(validated_df.select("prediction", "indexedLabel"))))
                                                                          


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Show some of the images the model failed to classify.

# COMMAND ----------

misclassified_df = validated_df.select('image', 'label', 'predictedLabel').filter(col('label') != col('predictedLabel')).limit(10)
display(misclassified_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Operationalize and manage
# MAGIC 
# MAGIC There are many options for operationalizing a trained model. The most basic one is to use the model as a data frame transformer that processes an input DataFrame and returns an output DataFrame with additional columns added that represent predictions - in our case a label describing the image, and a probability associated with the label. 
# MAGIC 
# MAGIC The other options are:
# MAGIC - Exporting using MLeap
# MAGIC - Operationalizng with Azure Machine Learning
# MAGIC - Wrapping in Spark SQL UDF
# MAGIC 
# MAGIC Note that not all options are currently supported for all training workflows. Specifically, there are limitations when using Deep Learning Pipelines, as the technology is still in early stages of development.  
# MAGIC 
# MAGIC In the next step of the lab, you will learn how to apply a model as a batch transformer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serialize and save the model
# MAGIC Currently, you cannot serialize the DeepImageFeaturizer stage. Since DeepImageFeaturizer does not have any trainable parameters this is not a major issue. You simply remove DeepImageFeaturizer before serialization and add it explicitly to the pipeline when you load the model at the later time. You also don't need the stage that converts a string label to a numeric one. It is not needed during inference.

# COMMAND ----------

# Remove label conversion stage
model.stages.pop(0)
# Remove DeepImageFeaturizer stage
model.stages.pop(0)
# Serialize and save the model
save_model_path = '/models/landclassifier'
model.write().overwrite().save(save_model_path)
model.stages

# COMMAND ----------

# MAGIC %md
# MAGIC The model is now persisted to a disk. If you use a model management solution like Azure ML you can track it as a configuration management item.
# MAGIC 
# MAGIC If at some later time you want to use the model for scoring (inference) you can load it from the disk.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Load the model
# MAGIC 
# MAGIC As noted before, the DeepImageFeaturizer stage has to be added to the restored model pipeline.

# COMMAND ----------

from pyspark.ml import PipelineModel

landclassifier = PipelineModel.read().load(save_model_path)
featurizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="ResNet50")
landclassifier.stages.insert(0, featurizer)
landclassifier.stages

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can now use the loaded model for batch inference.

# COMMAND ----------

test_img_df = ImageSchema.readImages(img_dir + 'test', recursive=True)
scored_img_df = landclassifier.transform(test_img_df)

# COMMAND ----------

display(scored_img_df.select('image', 'predictedLabel').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## THE END

# COMMAND ----------

