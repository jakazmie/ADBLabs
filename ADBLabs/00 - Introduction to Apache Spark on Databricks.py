# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##![Spark Logo Tiny](https://kpistoropen.blob.core.windows.net/collateral/roadshow/logo_spark_tiny.png) What is Azure Databricks?
# MAGIC 
# MAGIC Azure Databricks is an Apache Spark based Unified Analytics Platform optimized for Microsoft Azure. Designed with the founders of Apache Spark, Databricks is integrated with Azure to provide one-click setup, streamlined workflows, and an interactive workspace that enables collaboration between data scientists, data engineers, and business analysts.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## What is Apache Spark?
# MAGIC 
# MAGIC Apache Spark is a unified *computing engine* and a set of *libraries* for distributed data processing on compute clusters. Spark supports multiple widely used programming languages (Python, Java, Scala and R), includes libraries for diverse tasks ranging from SQL to streaming and machine learning.
# MAGIC 
# MAGIC ![Spark](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/SparkAPI.png)
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC ## Apache Spark Architecture
# MAGIC 
# MAGIC Apache Spark allows you to treat many machines as one machine and this is done via a master-worker type architecture where there is a `driver` or master node in the cluster, accompanied by `worker` nodes. The master sends work to the workers and either instructs them to pull to data from memory or from disk (or from another data source like Azure Blob Storage or Azure Data Lake).
# MAGIC 
# MAGIC The diagram below shows an example Apache Spark cluster, basically there exists a Driver node that communicates with executor nodes. Each of these executor nodes have slots which are logically like execution cores. 
# MAGIC 
# MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/videoss_logo.png)
# MAGIC 
# MAGIC The Driver sends Tasks to the empty slots on the Executors when work has to be done:
# MAGIC 
# MAGIC ![spark-architecture](http://training.databricks.com/databricks_guide/gentle_introduction/spark_cluster_tasks.png)
# MAGIC 
# MAGIC You can view the details of your Apache Spark application in the Apache Spark web UI.  The web UI is accessible in Databricks by going to "Clusters" and then clicking on the "View Spark UI" link for your cluster, it is also available by clicking at the top left of this notebook where you would select the cluster to attach this notebook to. In this option will be a link to the Apache Spark Web UI.
# MAGIC 
# MAGIC At a high level, every Apache Spark application consists of a driver program that launches various parallel operations on executor Java Virtual Machines (JVMs) running either in a cluster or locally on the same machine. In Databricks, the notebook interface is the driver program.  This driver program contains the main loop for the program and creates distributed datasets on the cluster, then applies operations (transformations & actions) to those datasets.
# MAGIC Driver programs access Apache Spark through a `SparkSession` object regardless of deployment location.
# MAGIC 
# MAGIC ## Apache Spark in Azure Databricks
# MAGIC Azure Databricks builds on the capabilities of Spark by providing a zero-management cloud platform that includes:
# MAGIC 
# MAGIC - Fully managed Spark clusters
# MAGIC - An interactive workspace for exploration and visualization
# MAGIC - A platform for powering your favorite Spark-based applications
# MAGIC 
# MAGIC 
# MAGIC ![UAP](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/azure-databricks-overview.png)
# MAGIC 
# MAGIC ### Fully managed Apache Spark clusters in the cloud
# MAGIC Azure Databricks has a secure and reliable production environment in the cloud, managed and supported by Spark experts. You can:
# MAGIC 
# MAGIC - Create clusters in seconds.
# MAGIC - Dynamically autoscale clusters up and down, including serverless clusters, and share them across teams.
# MAGIC - Use clusters programmatically by using the REST APIs.
# MAGIC - Use secure data integration capabilities built on top of Spark that enable you to unify your data without centralization.
# MAGIC - Get instant access to the latest Apache Spark features with each release.
# MAGIC 
# MAGIC ### Databricks Runtime
# MAGIC 
# MAGIC The Databricks Runtime is built on top of Apache Spark and is natively built for the Azure cloud.
# MAGIC 
# MAGIC With the high-performance cluster option, Azure Databricks completely abstracts out the infrastructure complexity and the need for specialized expertise to set up and configure your data infrastructure. The high performance cluster option helps data scientists iterate quickly as a team.
# MAGIC 
# MAGIC For data engineers, who care about the performance of production jobs, Azure Databricks provides a Spark engine that is faster and performant through various optimizations at the I/O layer and processing layer (Databricks I/O).
# MAGIC 
# MAGIC ### Workspace for collaboration
# MAGIC Through a collaborative and integrated environment, Azure Databricks streamlines the process of exploring data, prototyping, and running data-driven applications in Spark.
# MAGIC 
# MAGIC - Determine how to use data with easy data exploration.
# MAGIC - Document your progress in notebooks in R, Python, Scala, or SQL.
# MAGIC - Visualize data in a few clicks, and use familiar tools like Matplotlib, ggplot, or d3.
# MAGIC - Use interactive dashboards to create dynamic reports.
# MAGIC - Use Spark and interact with the data simultaneously.
# MAGIC 
# MAGIC ### Enterprise security
# MAGIC Azure Databricks provides enterprise-grade Azure security, including Azure Active Directory integration, role-based controls, and SLAs that protect your data and your business.
# MAGIC 
# MAGIC - Integration with Azure Active Directory enables you to run complete Azure-based solutions using Azure Databricks.
# MAGIC - Azure Databricks roles-based access enables fine-grained user permissions for notebooks, clusters, jobs, and data.
# MAGIC - Enterprise-grade SLAs.
# MAGIC 
# MAGIC ### Integration with Azure services
# MAGIC Azure Databricks integrates deeply with Azure databases and stores: SQL Data Warehouse, Cosmos DB, Data Lake Store, and Blob Storage.
# MAGIC 
# MAGIC ### Integration with Power BI
# MAGIC Through rich integration with Power BI, Azure Databricks allows you to discover and share your impactful insights quickly and easily. You can use other BI tools as well, such as Tableau Software via JDBC/ODBC cluster endpoints.
# MAGIC 
# MAGIC 
# MAGIC ## Azure Databricks Architecture
# MAGIC 
# MAGIC 
# MAGIC ![ADB Arch](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/adbarch.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Azure Databricks Terminology
# MAGIC 
# MAGIC ![ADB](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/adb_concepts.png)
# MAGIC 
# MAGIC Azure Databricks has key concepts that are important to understand. You'll notice that many of these line up with the links and icons that you'll see on the left side of the Databricks web application UI. These together define the fundamental tools that Databricks provides to you as an end user. They are available both in the web application UI as well as the REST API.
# MAGIC 
# MAGIC -   ****Workspaces****
# MAGIC     -   Workspaces allow you to organize all the work that you are doing on Databricks. Like a folder structure in your computer, it allows you to save ****notebooks**** and ****libraries**** and share them with other users. Workspaces are not connected to data and should not be used to store data. They're simply for you to store the ****notebooks**** and ****libraries**** that you use to operate on and manipulate your data with.
# MAGIC -   ****Notebooks****
# MAGIC     -   Notebooks are a set of any number of cells that allow you to execute commands. Cells hold code in any of the following languages: `Scala`, `Python`, `R`, `SQL`, or `Markdown`. Notebooks have a default language, but each cell can have a language override to another language. This is done by including `%[language name]` at the top of the cell. For instance `%python`. We'll see this feature shortly.
# MAGIC     -   Notebooks need to be connected to a ****cluster**** in order to be able to execute commands however they are not permanently tied to a cluster. This allows notebooks to be shared via the web or downloaded onto your local machine.
# MAGIC     -   Here is a demonstration video of [Notebooks](http://www.youtube.com/embed/MXI0F8zfKGI).
# MAGIC     -   ****Dashboards****
# MAGIC         -   ****Dashboards**** can be created from ****notebooks**** as a way of displaying the output of cells without the code that generates them. 
# MAGIC     - ****Notebooks**** can also be scheduled as ****jobs**** in one click either to run a data pipeline, update a machine learning model, or update a dashboard.
# MAGIC -   ****Libraries****
# MAGIC     -   Libraries are packages or modules that provide additional functionality that you need to solve your business problems. These may be custom written Scala or Java jars; python eggs or custom written packages. You can write and upload these manually or you may install them directly via package management utilities like pypi or maven.
# MAGIC -   ****Tables****
# MAGIC     -   Tables are structured data that you and your team will use for analysis. Tables can exist in several places. Tables can be stored on Azure Blob Storage, Azure Data Lake, they can also be stored on the cluster that you're currently using, or they can be cached in memory. [For more about tables see the documentation](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#02%20Product%20Overview/07%20Tables.html).
# MAGIC -   ****Clusters****
# MAGIC     -   Clusters are groups of computers that you treat as a single computer. In Databricks, this means that you can effectively treat 20 computers as you might treat one computer. Clusters allow you to execute code from ****notebooks**** or ****libraries**** on set of data. That data may be raw data located on S3 or structured data that you uploaded as a ****table**** to the cluster you are working on. 
# MAGIC     - It is important to note that clusters have access controls to control who has access to each cluster.
# MAGIC     -   Here is a demonstration video of [Clusters](http://www.youtube.com/embed/2-imke2vDs8).
# MAGIC -   ****Jobs****
# MAGIC     -   Jobs are the tool by which you can schedule execution to occur either on an already existing ****cluster**** or a cluster of its own. These can be ****notebooks**** as well as jars or python scripts. They can be created either manually or via the REST API.
# MAGIC     -   Here is a demonstration video of [Jobs](<http://www.youtube.com/embed/srI9yNOAbU0).
# MAGIC -   ****Apps****
# MAGIC     -   Apps are third party integrations with the Databricks platform. These include applications like Tableau.
# MAGIC 
# MAGIC 
# MAGIC You are ready to move to the hands-on labs. Let's start by creating a cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create a Spark cluster in Azure Databricks
# MAGIC 
# MAGIC 
# MAGIC 1. From the main page of Azure Databricks web UI click **Cluster**.
# MAGIC 
# MAGIC 2. In the New cluster page, provide the values to create a cluster. Accept all other default values other than the following:
# MAGIC   - Enter a name for you cluster
# MAGIC   - Create a cluster with 4.3 runtime
# MAGIC   - Set Python Version to 3
# MAGIC 
# MAGIC 
# MAGIC ![Create cluster](https://github.com/jakazmie/images-for-hands-on-labs/raw/master/create_cluster.png)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Install `sparklr`
# MAGIC 
# MAGIC In some labs, we will use `sparklyr`. We will install the latest version of sparklyr from CRAN. This might take couple minutes because it downloads and installs +10 dependencies.
# MAGIC 
# MAGIC You only need to do installation once on a cluster. After installation, all other notebooks attached to that cluster can import and use sparklyr.

# COMMAND ----------

# MAGIC %r
# MAGIC # Install sparklyr
# MAGIC # Installing latest version of Rcpp
# MAGIC install.packages("Rcpp") 
# MAGIC 
# MAGIC if (!require("sparklyr")) {
# MAGIC   install.packages("sparklyr")  
# MAGIC }

# COMMAND ----------

