# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Databricks integration with Azure Data Factory
# MAGIC 
# MAGIC In this lab, you use the Azure portal to create Azure Data Factory pipeline that executes a Databricks notebook against the Databricks jobs cluster. The pipeline also passes Azure Data Factory parameters to the Databricks notebook during execution.
# MAGIC 
# MAGIC You perform the following steps in this lab:
# MAGIC - Create a data factory
# MAGIC - Create a pipeline that uses Databricks Notebook Activity
# MAGIC - Trigger a pipeline run
# MAGIC - Monitor the pipeline run

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a data factory
# MAGIC 
# MAGIC 1. Launch Microsoft Edge or Google Chrome web browser. Currently, Data Factory UI is supported only in Microsoft Edge and Google Chrome web browsers.
# MAGIC 
# MAGIC 2. Select **Create a resource** on the left menu, select Analytics, and then select Data Factory.
# MAGIC 
# MAGIC    ![Create ADF](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/new-azure-data-factory-menu.png)
# MAGIC 
# MAGIC 3. In the **New data factory pane**, enter **Your Data Factory Name** under **Name**.
# MAGIC 
# MAGIC    The name of the Azure data factory must be globally unique. 
# MAGIC    
# MAGIC 4. For Subscription, select your Azure subscription in which you want to create the data factory.
# MAGIC 
# MAGIC 5. For **Resource Group**, select **Create new** and enter the name of a resource group.
# MAGIC 
# MAGIC 6. For **Version**, select **V2**.
# MAGIC 
# MAGIC 7. For **Location**, select **West US 2**.
# MAGIC 
# MAGIC 8. Select **Create**.
# MAGIC 
# MAGIC 9. After the provision is complete, you see the **Data Factory** page. Select the **Author & Monitor** tile to start the Data Factory UI application on a separate tab.
# MAGIC 
# MAGIC     ![ADF GUI](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image4.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a pipline with Databricks activity
# MAGIC 
# MAGIC In this section, you author a Databricks pipeline that includes Databricks Activity
# MAGIC 
# MAGIC ### Create an Azure Databricks linked service
# MAGIC 
# MAGIC 1. On the **Let's get started** page, switch to the **Author** tab in the left panel.
# MAGIC 
# MAGIC     ![Create Linked Service](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/get-started-page.png)
# MAGIC     
# MAGIC 2. Select **Connections** at the bottom of the window, and then select **+ New**.
# MAGIC 
# MAGIC     ![Connections](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image6.png)
# MAGIC     
# MAGIC 3. In the **New Linked Service** window, select **Compute > Azure Databricks**, and then select **Continue**.
# MAGIC 
# MAGIC    ![Linked Service](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image7.png)
# MAGIC    
# MAGIC 4. In the **New Linked Service** window, complete the following steps:
# MAGIC   - For **Name**, enter *AzureDatabricks_LinkedService*
# MAGIC   - Select the appropriate **Databricks workspace** that you will run your notebook in
# MAGIC   - For **Select cluster**, select **New job cluster**   d. For **Domain/Region**, info should auto-populate
# MAGIC   - For **Access Token**, generate it from Azure Databricks workspace. You can find the steps [here](https://docs.databricks.com/api/latest/authentication.html#generate-token)
# MAGIC   - For **Cluster version**, select **5.0**
# MAGIC   - For **Cluster node type**, select **Standard_D3_v2** under **General Purpose (HDD)** category
# MAGIC   - For **Python Version**, select **3**
# MAGIC   - For **Workers**, enter **2**
# MAGIC   - Select **Finish**
# MAGIC   
# MAGIC      ![Linked service](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/new-databricks-linkedservice.png)
# MAGIC      
# MAGIC ### Create a pipeline
# MAGIC  
# MAGIC 1. Select **Create resource**, and the select **Pipeline** on the menu
# MAGIC     
# MAGIC     ![Create pipeline](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image9.png)
# MAGIC     
# MAGIC 1. Change the name of the pipeline to *ADBNotebook*.
# MAGIC 
# MAGIC 1. Create **parameters** to be used in the **Pipeline**. Later you pass these parameters to the Databricks Notebook Activity. 
# MAGIC   - In the empty pipeline, click on the **Parameters** tab, then **New** and name it as **sas_key**. Use **string** as **Type**.
# MAGIC   - Repeat for **storage_account**, **container**, **output_path**.
# MAGIC     
# MAGIC     ![Parameters](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image10.png)
# MAGIC     
# MAGIC 1. In the **Activities** toolbox, expand **Databricks**. Drag the **Notebook** activity from the **Activities** toolbox to the pipeline designer surface.
# MAGIC 
# MAGIC     ![Activity](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/new-adf-pipeline.png)
# MAGIC     
# MAGIC 1. In the properties for the **Databricks Notebook** activity window at the bottom, complete the following steps:
# MAGIC   - Rename the activity to *LoadDataToADB*
# MAGIC   - Switch to the **Azure Databricks** tab.
# MAGIC   - Select **AzureDatabricks_LinkedService**
# MAGIC   - Switch to the **Settings** tab
# MAGIC   - Browse to select a Databricks **Notebook path**. Navigate to **ADBLabs** in you home folder. Select **load_parquet** in **Runnable** folder.
# MAGIC   - Under **Base Parameters**, select **+ New**.
# MAGIC   - Name the parameter as **SAS_KEY** and provide the value as expression **@pipeline().parameters.sas_key**
# MAGIC   - Repeat for other parameters defined in the **load_parquet** notebook: STORAGE_ACCOUNT, CONTAINER, OUTPUT_PATH
# MAGIC 
# MAGIC 1. To validate the pipeline, select the **Validate** button on the toolbar. To close the validatian window, select **>>** button.
# MAGIC 
# MAGIC 1. Select **Publish All**. The Data Factory UI publishes entities (linked serices and pipeline) to the Azure Data Factory service.
# MAGIC 
# MAGIC     ![Publish](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image19.png)
# MAGIC     
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Run the pipeline
# MAGIC 
# MAGIC ### Trigger the pipeline run
# MAGIC 
# MAGIC Select **Trigger** on the toolbar, and then select **Trigger Now**
# MAGIC 
# MAGIC ![Trigger](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image20.png)
# MAGIC 
# MAGIC The **Pipeline Run** dialog box asks for the pipeline parameters. Use the following values:
# MAGIC - **sas_key:** ?sv=2017-11-09&ss=bf&srt=sco&sp=rl&se=2019-04-07T04:13:15Z&st=2018-11-22T21:13:15Z&spr=https&sig=LiKQUVSxgVtB%2FtkfZ48QYmFYSwhk9cXcT0Woji0eKUQ%3D
# MAGIC - **storage account:** azureailabs
# MAGIC - **container:** chicago-crime
# MAGIC - **output_path:** /datasets/chicagocrimes.parquet
# MAGIC 
# MAGIC 
# MAGIC ### Monitor the pipeline run
# MAGIC 
# MAGIC 1. Switch to the **Monitor** tab. Confirm that you see a pipeline run. It takes approximately 5-8 minutes to create a Databricks job cluster, where the notebook is executed.
# MAGIC 
# MAGIC ![Monitor](https://docs.microsoft.com/en-us/azure/data-factory/media/transform-data-using-databricks-notebook/databricks-notebook-activity-image22.png)
# MAGIC 
# MAGIC 2. Select **Refresh** periodically to check the status of the pipeline run.
# MAGIC 
# MAGIC 3. To see activity runs associated with the pipeline run, select **View Activity Runs** in the **Actions** column.
# MAGIC 
# MAGIC You can switch back to the pipeline runs view by selecting the **Pipelines** link at the top.
# MAGIC 
# MAGIC ### Verify the output
# MAGIC 
# MAGIC 
# MAGIC You can log on to the **Azure Databricks workspace**, got to **Cluster** and you can see the **Job** status as *pending execution*, *running*, or *terminated*.

# COMMAND ----------

