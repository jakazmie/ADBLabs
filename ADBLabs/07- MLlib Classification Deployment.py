# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Spark ML model operationalization with Azure Machine Learning service.
# MAGIC 
# MAGIC In this lab, you will operationalize the Spark ML model developed in Lab 03 as a REST web service running in Azure Container Instance.
# MAGIC Azure Machine Learning service helps you orchestrate machine learning workflows using the architecture depicted on the below diagram.
# MAGIC 
# MAGIC ![AML workflow](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/amlarch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Install Azure ML SDK 
# MAGIC 
# MAGIC Before you can use Azure ML service features from Azure Databricks, you need to install Azure ML SDK as Azure Databricks Library. Follow this instructions:
# MAGIC 
# MAGIC https://docs.databricks.com/user-guide/libraries.html 
# MAGIC 
# MAGIC and add `azureml-sdk[databricks]` as your PyPi package. You can select the option to attach the library to all clusters or just one cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check SDK Version

# COMMAND ----------

import azureml.core

print("SDK version:", azureml.core.VERSION)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Connect to Azure ML workspace
# MAGIC 
# MAGIC Follow the instructor to create Azure ML Workspace using Azure Portal. 
# MAGIC 
# MAGIC After the workspace has been provisioned, execute the below cells to connect to the workspace and save connection information on a driver node.

# COMMAND ----------

# Set connection parameters
subscription_id = "<your subscription>"
resource_group = "<your resource gropup>"
workspace_name = "<your workspace name>"


# COMMAND ----------

# Set connection parameters
subscription_id = "952a710c-8d9c-40c1-9fec-f752138cc0b3"
resource_group = "jkaml"
workspace_name = "jkaml"


# COMMAND ----------

from azureml.core import Workspace# Connect to the workspace

from azureml.core import Workspace

ws = Workspace(workspace_name = workspace_name,
               subscription_id = subscription_id,
               resource_group = resource_group)

# persist the subscription id, resource group name, and workspace name in aml_config/config.json.
ws.write_config()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Review the AML config file.

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /databricks/driver/aml_config/config.json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Register your model 
# MAGIC 
# MAGIC One of the key features of Azure Machine Learning service is **Model Registry**. You can use model registry to manage versions of models including arbitrary meta data about the models.
# MAGIC 
# MAGIC Before you call the AML model register API you need to copy the model to the driver node, as the model register API searches for model files in the local (driver) file system.

# COMMAND ----------

import os

model_dbfs_path = '/models/churn_classifier'
model_name = 'ChurnClassifierML'
model_local = 'file:' + os.getcwd() + '/' + model_name

dbutils.fs.cp(model_dbfs_path, model_local, True)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC ls -la .

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC You can now register the model.

# COMMAND ----------

from azureml.core.model import Model

mymodel = Model.register(model_path=model_name, 
                         model_name=model_name,
                         description='Spark ML classifier model for customer churn prediction',
                         workspace=ws
                        )

print(mymodel.name, mymodel.description, mymodel.version)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC The model has been registered with the model registry. The next step is to deploy the model to Azure Container Instance.
# MAGIC 
# MAGIC ## Deploy the model to ACI
# MAGIC 
# MAGIC To build the correct environment for ACI deployment, you need to provide the following:
# MAGIC * A scoring script that invokes the model
# MAGIC * An environment file to show what packages need to be installed
# MAGIC * A configuration file to build the ACI
# MAGIC * The serialized model 
# MAGIC 
# MAGIC ### Create scoring script
# MAGIC 
# MAGIC Create the scoring script, called score.py, used by the web service call to invoke the model.
# MAGIC 
# MAGIC You must include two required functions in the scoring script:
# MAGIC * The `init()` function, which loads the model into a global object. This function is run only once when the Docker container is started. 
# MAGIC 
# MAGIC * The `run(input_data)` function uses the model to predict a value based on the input data. Inputs and outputs to the run typically use JSON for serialization and de-serialization, but other formats can be used.

# COMMAND ----------

# MAGIC %%writefile score.py
# MAGIC 
# MAGIC import json
# MAGIC import pyspark
# MAGIC from azureml.core.model import Model
# MAGIC from pyspark.ml import PipelineModel
# MAGIC 
# MAGIC 
# MAGIC def init():
# MAGIC     try:
# MAGIC         # One-time initialization of PySpark and predictive model
# MAGIC         
# MAGIC         global trainedModel
# MAGIC         global spark
# MAGIC         
# MAGIC         spark = pyspark.sql.SparkSession.builder.appName("Churn prediction").getOrCreate()
# MAGIC         model_name = "<<model_name>>" 
# MAGIC         model_path = Model.get_model_path(model_name)
# MAGIC         trainedModel = PipelineModel.load(model_path)
# MAGIC     except Exception as e:
# MAGIC         trainedModel = e
# MAGIC 
# MAGIC def run(input_json):
# MAGIC     if isinstance(trainedModel, Exception):
# MAGIC         return json.dumps({"trainedModel":str(trainedModel)})
# MAGIC       
# MAGIC     try:
# MAGIC         sc = spark.sparkContext
# MAGIC         input_list = json.loads(input_json)
# MAGIC         input_rdd = sc.parallelize(input_list)
# MAGIC         input_df = spark.read.json(input_rdd)
# MAGIC     
# MAGIC         # Compute prediction
# MAGIC         prediction = trainedModel.transform(input_df)
# MAGIC         #result = prediction.first().prediction
# MAGIC         predictions = prediction.collect()
# MAGIC 
# MAGIC         #Get each scored result
# MAGIC         preds = [str(x['prediction']) for x in predictions]
# MAGIC         # result = ",".join(preds)
# MAGIC         result = preds
# MAGIC     except Exception as e:
# MAGIC         result = str(e)
# MAGIC     return json.dumps({"result":result})        

# COMMAND ----------

# MAGIC %sh
# MAGIC cat score.py

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Substitue the actual *model name* in the script file.

# COMMAND ----------

script_file_name = 'score.py'

with open(script_file_name, 'r') as cefr:
    content = cefr.read()
    
with open(script_file_name, 'w') as cefw:
    cefw.write(content.replace('<<model_name>>', mymodel.name))

# COMMAND ----------

# MAGIC %sh
# MAGIC cat score.py

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create a docker image encapsulating the model
# MAGIC 
# MAGIC The docker image that encapsulates our model is be based on a standard AML image that contains the PySpark runtime and a web service wrapper. It must also include the scoring script, any dependencies required by the scoring script, and `azureml-defaults`.

# COMMAND ----------

from azureml.core.conda_dependencies import CondaDependencies 

myenv = CondaDependencies()

with open("myenv.yml","w") as f:
    f.write(myenv.serialize_to_string())
    
# Review Conda dependencies file
with open("myenv.yml","r") as f:
    print(f.read())

# COMMAND ----------

from azureml.core.image import ContainerImage, Image

runtime = "spark-py"
scoring_script = "score.py"

# Configure the image
image_config = ContainerImage.image_configuration(execution_script=scoring_script, 
                                                  runtime=runtime, 
                                                  conda_file="myenv.yml",
                                                  description="Churn prediction web service",
                                                  tags={"Classifier": "GBT"})

# Create image
image = Image.create(name = "churn-classifier",
                     # this is the model object 
                     models = [mymodel],
                     image_config = image_config, 
                     workspace = ws)

image.wait_for_creation(show_output = True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Define ACI configuration
# MAGIC 
# MAGIC Create a deployment configuration file and specify the number of CPUs and gigabyte of RAM needed for your ACI container. The default is 1 core and 1 gigabyte of RAM.  In this lab we will use the defaults but you should always go through the proper performance plannig exercise to find the right configuration.

# COMMAND ----------

from azureml.core.webservice import AciWebservice

aciconfig = AciWebservice.deploy_configuration(cpu_cores=1, 
                                               memory_gb=1, 
                                               tags={"Model": "GBT"}, 
                                               description='Predict customer churn')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Deploy in ACI
# MAGIC 
# MAGIC Deploy the image as a web service in Azure Container Instance.

# COMMAND ----------

from azureml.core.webservice import Webservice

aci_service_name = 'churn-classifier'
print(aci_service_name)
aci_service = Webservice.deploy_from_image(deployment_config = aciconfig,
                                           image = image,
                                           name = aci_service_name,
                                           workspace = ws)
aci_service.wait_for_deployment(True)

# COMMAND ----------

print(aci_service.get_logs())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test the prediction web service
# MAGIC 
# MAGIC The web service encapsulating the model has been started and is accessible using the following URL.

# COMMAND ----------

print(aci_service.scoring_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC To test the service we will use 5 rows from the testing dataset that was saved as a parquet file in the previous lab. As you recall, the `run` function in the scoring script assumes that the data is formatted as JSON.

# COMMAND ----------

import json

# Read 5 rows fro the test dataset
test_data = spark.read.parquet("/datasets/churn_test_data").limit(5)

# Convert it to JSON
test_json = json.dumps(test_data.toJSON().collect())
print(test_json)


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Call the web service.

# COMMAND ----------

aci_service.run(input_data=test_json)

# COMMAND ----------

print(aci_service.get_logs())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Clean up

# COMMAND ----------

#Delete service

aci_service.delete()