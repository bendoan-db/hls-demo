# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "reset_all_data")
dbutils.widgets.text("database_name", "doan_demo_database")
dbutils.widgets.text("table_prefix", "hls")

# COMMAND ----------

# MAGIC %run ./init/setup $reset_all_data=$reset_all_data $database_name=$database_name $table_prefix=$table_prefix

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from csv.`dbfs:/user/ben.doan@databricks.com/tmp/hls-demo/raw-insurance-data/insurance.csv`

# COMMAND ----------

# DBTITLE 1,Load Raw Files into a Bronze Table using Autoloader
rawFilesPath = "dbfs:/user/ben.doan@databricks.com/tmp/hls-demo/raw-insurance-data/"

rawDataDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("cloudFiles.maxFilesPerTrigger", 1) \
                .schema("age integer, sex string, bmi double, children integer, smoker string, region string, charges double") \
                .load(rawFilesPath)

rawDataDF.writeStream \
        .option("ignoreChanges", "true") \
        .option("mergeSchema", "true") \
        .trigger(processingTime='1 seconds') \
        .table(bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Load Raw Files into a Bronze Table using COPY INTO
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {bronze_table_name} (age integer, sex string, bmi double, children integer, smoker string, region string, charges double) using delta;
""")

spark.sql(f"""
  COPY INTO {bronze_table_name}
  FROM '{rawFilesPath}'
  FILEFORMAT = CSV
  FORMAT_OPTIONS('header'='true', 'inferSchema' = 'true')
""")

# COMMAND ----------

# DBTITLE 1,Featurization Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, OneHotEncoder

#load the data
bronzeDF = spark.read.table(bronze_table_name)

#complete basic imputation and featurization (string indexing + one hot encoding)
numFeatures = [column.name for column in bronzeDF.schema.fields if column.dataType != StringType()]
catFeatures = [column.name for column in bronzeDF.schema.fields if column.dataType == StringType()]

#create lists of categorical and numerical columns for encoding
indexedFeatures = [column + "_index" for column in catFeatures]
encodedFeatures = [column + "_encoded" for column in catFeatures]
finalFeatures = numFeatures + encodedFeatures

numImputer = Imputer()\
  .setStrategy('median')\
  .setInputCols(numFeatures)\
  .setOutputCols(numFeatures)

stringIndexer = StringIndexer(inputCols=catFeatures, outputCols=indexedFeatures, handleInvalid="skip")
oneHotEncoder = OneHotEncoder(inputCols=indexedFeatures, outputCols=endodedFeatures)

#create reproducable featurization pipeline
pipeline = Pipeline(stages=[numImputer, stringIndexer, oneHotEncoder])
fitted_pipeline=pipeline.fit(bronzeDF)

#transform bronze data to silver feature table
silverFeaturesDF = fitted_pipeline.transform(bronzeDF).select(finalFeatures)

# COMMAND ----------

silverFeaturesDF.write.format("delta").saveAsTable(silver_table_features_name)

# COMMAND ----------


