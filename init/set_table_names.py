# Databricks notebook source
dbutils.widgets.text("database_name", "doan_demo_database")
dbutils.widgets.text("table_prefix", "hls")

# COMMAND ----------

database_name = dbutils.widgets.get("database_name")
table_prefix = dbutils.widgets.get("table_prefix")

# COMMAND ----------

# DBTITLE 1,Raw Tables
print("Creating raw table namespaces...")
raw_data_table_name = f"{database_name}.{table_prefix}" + "_raw_data"
print("Raw Table(s) Created: " + raw_data_table_name)

# COMMAND ----------

# DBTITLE 1,Bronze Tables
bronze_table_name = f"{database_name}.{table_prefix}" + "_bronze_data"
print("Bronze Table(s) Created: " + bronze_table_name)

# COMMAND ----------

# DBTITLE 1,Silver Tables
silver_table_name = f"{database_name}.{table_prefix}" + "_silver_data"
silver_table_features_name = f"{database_name}.{table_prefix}" + "_silver_features"
print("Silver Table(s) Created: " + silver_table_name + "\n" + silver_table_features_name)

# COMMAND ----------

# DBTITLE 1,Gold Tables
gold_auto_ml_table_name = f"{database_name}.{table_prefix}" + "_gold_automl"
gold_ml_table_name = f"{database_name}.{table_prefix}" + "_gold_ml"
print("Bronze Table(s) Created: " + gold_auto_ml_table_name + "\n" + gold_ml_table_name + "\n" + gold_auto_ml_table_name + "\n")

# COMMAND ----------

print("Creating table namespaces...")
print("Table namespaces created.")

# COMMAND ----------


