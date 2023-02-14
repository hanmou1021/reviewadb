# Databricks notebook source
# MAGIC %md
# MAGIC ### Read the csv file genereated before to read as DataFrame

# COMMAND ----------

# File location and type
file_location = "dbfs:/pcombined.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files.
df = (spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(file_location)
     )

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Create a new DataFrame from df to calculate the count for people who have the same Last Name

# COMMAND ----------

LastNameCounts = df.groupBy('LastName').count()
display(LastNameCounts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Save DataFrame LastNameCounts as a table in Database Tables (i.e. shown in the Data tab)

# COMMAND ----------

LastNameCounts.createOrReplaceTempView("test1")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table lastnamecounts as select * from test1

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Configure the parameters below to have the correct format (e.g., correct schema) for DataFrame

# COMMAND ----------

# TODO
# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files.
df2 = (spark.read.format(file_type)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load('/databricks-datasets/COVID/covid-19-data/us-counties.csv'))

display(df2)





# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Create a new DataFrame from covid to count the people in California who lives in the same county. 

# COMMAND ----------

df2.createOrReplaceTempView("covid")


# COMMAND ----------

# MAGIC %sql
# MAGIC create table covidcount as select county, state, count(fips) as cou from covid where state='California' group by county,state 

# COMMAND ----------

cali=spark.table('covidcount')

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Using display() configure a Pie Chart to show portions of people who live in the same county.

# COMMAND ----------

display(cali)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Read the 8 json files from people as DataFrame with corresponding schema.

# COMMAND ----------

df8=display(spark.read.option('multiline','true').json(['/FileStore/tables/0.json','/FileStore/tables/1.json','/FileStore/tables/2.json','/FileStore/tables/3.json',
                                                        '/FileStore/tables/4.json','/FileStore/tables/5.json','/FileStore/tables/6.json','/FileStore/tables/7.json']))

# COMMAND ----------

type(json)

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/1.json")
dbutils.fs.rm("/FileStore/tables/2.json")
dbutils.fs.rm("/FileStore/tables/3.json")
dbutils.fs.rm("/FileStore/tables/4.json")
dbutils.fs.rm("/FileStore/tables/5.json")
dbutils.fs.rm("/FileStore/tables/6.json")
dbutils.fs.rm("/FileStore/tables/7.json")
dbutils.fs.rm("/FileStore/tables/8.json")


# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/t1.json")

# COMMAND ----------


