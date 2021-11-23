# Databricks notebook source
# MAGIC %md
# MAGIC # Migrate Delta Table to New Location
# MAGIC ### What?
# MAGIC Title ^
# MAGIC ### Why?
# MAGIC Many users desire a way to re-organize their Delta Tables as they evolve with Databricks and see new patterns. We also want to help 
# MAGIC     customers migrate away from DBFS managed tables and FUSE Mounts to prepare them for Unity Catalog. 
# MAGIC ### Who?
# MAGIC Customers using DBFS Tables/FUSE Mounts
# MAGIC Customers who ask for a way to move a Delta Lake table to a new location
# MAGIC Persona: Data Engineers can run this interactively and get feedback from the cell output OR DE/Ops could script this out using Widget 
# MAGIC     Paremeters to migrate many tables at once. 
# MAGIC ### How?
# MAGIC Delta Lake includes a great feature called [Delta Clone](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-clone.html).
# MAGIC This notebook implements Delta Clone and Delta Lake w/ the PySpark and SparkSQL APIs to facilitate a migration of your Delta Lake 
# MAGIC     Table and associated Hive Metadata to a new location. 
# MAGIC ### Caveats
# MAGIC - Currently Delta Clone does not support migrating table history
# MAGIC - I have not tested this with FUSE Mounts yet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize the environment

# COMMAND ----------

# MAGIC %md
# MAGIC EXAMPLE BASE PATH FOR AZURE: 
# MAGIC ##### abfss://container-name@storage-account-name.dfs.core.windows.net/directory-name/my-metastore/gold-zone

# COMMAND ----------

dbutils.widgets.text("external_url", "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<directory-name>", label = "New Base Url")
dbutils.widgets.text("table_name", "", label = "Hive Table Name")
dbutils.widgets.text("db_name", "", label = "Database Name")
dbutils.widgets.dropdown("managed", "Yes", ["Yes","No"],"Moving a Managed Table?")
table_name = dbutils.widgets.get("table_name")
spark.sql("USE {}".format(dbutils.widgets.get("db_name")))
new_url = "{0}{1}".format(dbutils.widgets.get("external_url"),table_name)
managed = dbutils.widgets.get("managed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the correct target and source

# COMMAND ----------

location = spark.sql("DESCRIBE DETAIL {}".format(table_name)).collect()[0].location
print("{0} LOCATED AT {1} IS MOVING {2}".format(table_name, location, new_url))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clone the table

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE delta.`{0}` CLONE `{1}`
""".format(new_url, table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the data is all moved

# COMMAND ----------

old = spark.table(table_name)
new = spark.read.load(path=new_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure the old table matches the new table

# COMMAND ----------

display(old.exceptAll(new))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure the new table matches the old table

# COMMAND ----------

display(new.exceptAll(old))

# COMMAND ----------

# MAGIC %md
# MAGIC ## !!! DROP ORIGINAL TABLE & DATA !!!

# COMMAND ----------

spark.sql("""
DROP TABLE {}""".format(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### If table is not managed we have to also clean up the existing data

# COMMAND ----------

if managed == 'No':
  dbutils.fs.rm(location,True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register new table with hive

# COMMAND ----------

spark.sql("""
CREATE TABLE {0}
USING DELTA
LOCATION "{1}"
""".format(table_name, new_url))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify that we can query the cloned table by name

# COMMAND ----------

display(spark.sql("SELECT * FROM {}".format(table_name)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the new location is correct 

# COMMAND ----------

new_location = spark.sql("DESCRIBE DETAIL {}".format(table_name)).collect()[0].location
if new_location == new_url:
  print("The new table location matched the target.\nThe migration process is complete for {}".format(table_name))
else:
  print("The new table location does not match the target.")
  print("New Table Location: {}".format(new_location))
  print("Target Table Location: {}".format(new_url))     
  print("Try running the 'Register new table with Hive' step")

# COMMAND ----------


