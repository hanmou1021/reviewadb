# Databricks notebook source
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# COMMAND ----------

""" 1. python functions """
def convertCase(str):
    if str is None:
        return 'Null'
    else:
        resStr=""
        arr = str.split(" ")
        for x in arr:
            resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
        return resStr 

# Add python functions below

# COMMAND ----------

""" 2. Converting functions to UDFs """
convertUDF = udf(lambda z: convertCase(z))
# Add UDFs registration below

# COMMAND ----------

""" 3. [Optional] Using UDF on SQL """
spark.udf.register("convertUDF", convertCase, StringType())

# COMMAND ----------

######################################
# Create a new UDF in Utilities notebook
# Change field to 'Unknown' for Department that has None or broken value e.g., #$@#@.
def depchange(input):
    newinput=str(input)
    if input==None:
        return 'Unknown'
    elif ('$' or '#' or '@') in newinput:
        return 'Unknown'
    else:
        return newinput

# COMMAND ----------

dcUDF = udf(lambda z: depchange(z))

# COMMAND ----------

spark.udf.register("dcUDF", depchange, StringType())

# COMMAND ----------

def salarychange(input):
    try:
        newinput=int(input)
    except:
        return '$0'
    if newinput is None:
        return '$0'
    elif newinput<0:
        return '$0'
    else:
        return '$'+str(newinput)

# COMMAND ----------

sUDF = udf(lambda z: salarychange(z))
spark.udf.register("sUDF", salarychange, StringType())

# COMMAND ----------


