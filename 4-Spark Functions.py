# Databricks notebook source
covid = (spark.read
         .option("header", True)
         .option("inferSchema", True)
         .csv('/databricks-datasets/COVID/covid-19-data/us-counties.csv'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show the schema for covid DataFrame

# COMMAND ----------

covid.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select cases for the counties with each state greater than 100 million cases from covid DataFrame

# COMMAND ----------

from pyspark.sql.functions import *
county_count_df = (covid.groupBy(col('state'), col('county'))
                   .agg(sum('cases').alias('total_cases'))
                   .where(col('total_cases') > 10**8))
display(county_count_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: select state with most cases from covid DataFrame

# COMMAND ----------

covid.createOrReplaceTempView("ct")

# COMMAND ----------

# MAGIC %sql
# MAGIC select state,sum(cases) as totalcase from ct group by state order by totalcase desc limit 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: select the records for the latest day from covid DataFrame

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ct where date in (select distinct date from ct order by date desc limit 1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: merge the state and county column to "State-County" from covid DataFrame. <br>
# MAGIC ### The records should also be concatenated with "-", for example "New York | New York City" now is "New York-New York City".

# COMMAND ----------

# MAGIC %sql
# MAGIC create table newgeo as select *,concat(state,'-',county) as newgeoinfo from ct

# COMMAND ----------

lastdf=table('newgeo')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the Container in your Storage Account (the one you used before for logging on Azure Storage) to your DBFS

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://mountdatatry@adblogging111.blob.core.windows.net",
  mount_point = "/mnt/mounteddate",
  extra_configs = {"fs.azure.account.key.adblogging111.blob.core.windows.net":"zsyxoa1O/0W+ibSuOYS/t6WIRYGKqqtzNaJaCHfQdEqaGA1fVYnXoHdbxZn5FkkHVYaiSKBWTdpi+AStOqsyDg=="})

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: save the last dataframe to the Container in your Storage Account in .parquet files

# COMMAND ----------

lastdf.write.parquet('mnt/mounteddate/cvnewgeo3.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: read the .parquet files from the mount and read them as DataFrame

# COMMAND ----------

test=spark.read.parquet('/mnt/mounteddate/cvnewgeo3.parquet')
display(test)

# COMMAND ----------


