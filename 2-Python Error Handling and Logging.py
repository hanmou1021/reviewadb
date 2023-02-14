# Databricks notebook source
# MAGIC %md
# MAGIC ### If you have some suspicious code that may raise an exception, you can defend your program by placing the suspicious code in a try: block. 
# MAGIC ### After the try: block, include an except: statement, followed by a block of code which handles the problem as elegantly as possible.

# COMMAND ----------

xi = iter([1, 2, 3])

# COMMAND ----------

# Will give an error because xi only has 3 elements. 
# TODO: Have a try-except error handling so that we can skip this error and run the print statement.
while True:
    print(next(xi))
print("iterator looping complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assert takes a condition/expression and check if it’s true
# MAGIC ### When assertion fails, AssertionError exception is thrown.

# COMMAND ----------

# returns the sum only when a and b are two-digit numbers; otherwise return None
def addTwoDigitNumbers(a, b):
    if a/10>=1 and b/10>=1:
        return a+b
    else:
        return None

# COMMAND ----------

# Unit-testing addTwoDigitNumbers function
# assert only gives error when two items are NOT matched
assert(addTwoDigitNumbers(10, 20) == 30)
assert(addTwoDigitNumbers(0, 20) == None)
assert(addTwoDigitNumbers(0, 100) == None)
assert(addTwoDigitNumbers(20, 10) == None)
assert(addTwoDigitNumbers(100, 0) == None)

# COMMAND ----------

import logging
logging.basicConfig(filename='ss1.log', level=logging.INFO, force=True)

assert(addTwoDigitNumbers(10, 20) == 30)
assert(addTwoDigitNumbers(0, 20) == None)
assert(addTwoDigitNumbers(0, 100) == None)

open("sad.txt")

assert(addTwoDigitNumbers(20, 10) == None)
assert(addTwoDigitNumbers(100, 0) == None)


# COMMAND ----------

with open("ss1.log") as log:
    print(log.read())

# COMMAND ----------

# MAGIC %md
# MAGIC Python comes with a logging module in the standard library. <br>
# MAGIC To emit a log message, a caller first requests a named logger. <br>
# MAGIC The name can be used by the application to configure different rules for different loggers. <br>
# MAGIC This logger then can be used to emit simply-formatted messages at different log levels (DEBUG, INFO, ERROR, etc.), which again can be used by the application to handle messages of higher priority different than those of a lower priority.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment: For the above iteration and assertion, have a logging handling to record job completes or fails (with error message) using Azure Storage Logging.

# COMMAND ----------

import logging
from azure_storage_logging.handlers import TableStorageHandler

# configure the handler and add it to the logger
logger = logging.getLogger('logexample')
handler = TableStorageHandler(account_name='adblogging111',
                              account_key='zsyxoa1O/0W+ibSuOYS/t6WIRYGKqqtzNaJaCHfQdEqaGA1fVYnXoHdbxZn5FkkHVYaiSKBWTdpi+AStOqsyDg==',
                              protocol='https', 
                              table='logs', 
                              batch_size=100, extra_properties=None, partition_key_formatter=None,
                              row_key_formatter=None,is_emulated=False)
logger.addHandler(handler)

logger.info('info message')
logger.warning('warning message')
logger.error('error message')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment: Python Coding Practices

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of letters, e.g. ["a", "b", "a", "c", "b", "b"], 
# MAGIC return the ocrrencies of the elementes within that array, e.g. {"a": 2, "b": 3, "c": 1}.

# COMMAND ----------

# TODO
n=["a", "b", "a", "c", "b", "b"]
ns=list(set(n))
result={}

# COMMAND ----------

for i in ns:
    qq=0
    for k in range(0,len(n)):
        if n[k]==i:
            qq+=1
    result[i]=qq

# COMMAND ----------

result

# COMMAND ----------

# MAGIC %md
# MAGIC Given two arrays, subjects e.g. ["English", "Math"] and corresponding scores e.g. [88, 92], 
# MAGIC return the combined result in one array in descending order of scores e.g. [["Math", 92], ["English", 88]].

# COMMAND ----------

aa=["English", "Math"]
bb=[88, 92]
com=[]

dict={}
for i in range(0,len(aa)):
    dict[aa[i]]=bb[i]

# COMMAND ----------

bb=sorted(bb,reverse= True)
bb

# COMMAND ----------

# function to return key for any value
def get_key(val):
    for key, value in dict.items():
        if val == value:
            return key
 
    return "key doesn't exist"

# COMMAND ----------

com=[]
for i in range(0,len(bb)):
    q=[get_key(bb[i])]
    j=[bb[i]]
    com=com+[q+j]
com
    

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of URLs e.g. ["a.com/u=test&s=true","b.com/u=dev&s=false"], 
# MAGIC return an array of dictionaries of data parsed from URL e.g. 
# MAGIC [{url:"a.com", u:"test", s:"true"}, {url:"b.com", u:"dev", s:"false"}].

# COMMAND ----------

# TODO
import re
input=["a.com/u=test&s=true","b.com/u=dev&s=false"]
my_list = re.split(r'/|=|&', input[1])
comb=[]

for i in range(0,len(input)):
    my_list = re.split(r'/|=|&', input[i])
    dic={}
    dic["url"]=my_list[0]
    dic[my_list[1]]=my_list[2]
    dic[my_list[3]]=my_list[4]
    comb=comb+[dic]

# COMMAND ----------

comb

# COMMAND ----------

# MAGIC %md
# MAGIC Given an array of integers e.g. [2, 5, 10] and a power e.g. 3,
# MAGIC return the powered result of a dictionary in descending order based on the result 
# MAGIC e.g. {10: 100, 5: 125, 2: 8}

# COMMAND ----------

# TODO
dic={}
l=sorted([2,5,10],reverse=True)
for i in l:
    dic[i]=i**3

# COMMAND ----------

dic
