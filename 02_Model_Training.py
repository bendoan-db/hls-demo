# Databricks notebook source
dbutils.widgets.text("database_name", "doan_demo_database")
dbutils.widgets.text("table_prefix", "hls")

# COMMAND ----------

# MAGIC %run ./init/set_table_names $reset_all_data=$reset_all_data $database_name=$database_name $table_prefix=$table_prefix

# COMMAND ----------

silverFeatureTables = spark.read.table("doan_demo_database.hls_silver_features")

# COMMAND ----------

training, test = training_gold_dataset_indexed.limit(10000).randomSplit([0.9, 0.1])

#convert to pandas <- but don't worry! Spark 3.0 has built in support for Koalas and distributed pandas out of the box
train_pdf = training.toPandas()
test_pdf = test.toPandas()

X_train = train_pdf.drop(["charges"], axis=1)
X_test = test_pdf.drop(["charges"], axis=1)

y_train = train_pdf["status_indexed"]
y_test = test_pdf["status_indexed"]

# COMMAND ----------


