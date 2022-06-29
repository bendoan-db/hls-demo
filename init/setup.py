# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "reset_all_data")
dbutils.widgets.text("database_name", "doan_demo_database")
dbutils.widgets.text("table_prefix", "hls")

# COMMAND ----------

# MAGIC %run ./set_table_names $reset_all_data=$reset_all_data $database_name=$database_name $table_prefix=$table_prefix

# COMMAND ----------

# DBTITLE 1,Package imports
from pyspark.sql.functions import rand, input_file_name, from_json, col
from pyspark.sql.types import *
 
from pyspark.ml.feature import StringIndexer, StandardScaler, VectorAssembler
from pyspark.ml import Pipeline

#ML import
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.mllib.evaluation import MulticlassMetrics
from mlflow.utils.file_utils import TempDir
import mlflow.spark
import mlflow
import seaborn as sn
import pandas as pd
import matplotlib.pyplot as plt

from time import sleep
import re

# COMMAND ----------

# MAGIC %scala
# MAGIC /*
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC import java.time.LocalDateTime
# MAGIC 
# MAGIC val reset_all_data = (dbutils.widgets.get("reset_all_data") == "true")
# MAGIC 
# MAGIC if(reset_all_data == true) {
# MAGIC   val tmpFile = new File("/dbfs/user/" + current_user + "/tmp/hls-demo/raw-insurance-data/insurance.csv")
# MAGIC   println("Copying new data from source to /dbfs/user/" + current_user + "/tmp/hls-demo/raw-insurance-data/insurance.csv")
# MAGIC   FileUtils.copyURLToFile(new URL("https://raw.githubusercontent.com/stedy/Machine-Learning-with-R-datasets/master/insurance.csv"), tmpFile)
# MAGIC }
# MAGIC */

# COMMAND ----------

from datetime import datetime

database_name = dbutils.widgets.get("database_name")
reset_all_data = (dbutils.widgets.get("reset_all_data") == "true")
table_prefix = dbutils.widgets.get("table_prefix")
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
user_tmp_path = "dbfs:/user/" + current_user + "/tmp/hls-demo"
raw_data_path = "dbfs:/user/" + current_user + "/tmp/hls-demo/raw-insurance-data/insurance.csv"

# COMMAND ----------

spark.sql(f"""create database if not exists {database_name}""")

# COMMAND ----------

reset_all = dbutils.widgets.get("reset_all_data") == "true"
if reset_all_data:
  tables_list = spark.sql(f"""show tables in {database_name}""").select("tableName").filter(col("tableName").contains(table_prefix)).collect()
  for table in tables_list:
    table_name = table[0]
    print(f"Resetting Data... Deleting table {table_name}")
    spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")

# COMMAND ----------

# DBTITLE 1,Reset tables in user's database
spark.conf.set("spark.sql.streaming.checkpointLocation", user_tmp_path+"/checkpoint")

#Allow schema inference for auto loader
spark.conf.set("spark.databricks.cloudFiles.schemaInference.enabled", "true")

# COMMAND ----------

print("""
         1 1  Init Complete!
        (o_o)/
        <)  )
        /  \   
""")
