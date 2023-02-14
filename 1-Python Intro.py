# Databricks notebook source
# MAGIC %md
# MAGIC ## Native Datatypes in Python
# MAGIC ### 1. Python is Dynamically and Strongly typed
# MAGIC ### 2. You don’t need to explicitly declare the data type of variables in python
# MAGIC ### 3. Python will “guess” the datatype and keeps track of it internally.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bytes and byte arrays, e.g. a jpeg image file.

# COMMAND ----------

x = b'hello'
x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lists are ordered sequences of objects.

# COMMAND ----------

arr = ['Apple', 'Banana']
arr

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' in the array arr

# COMMAND ----------

arr.append('Cherry')
arr

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tuples are ordered, immutable sequences of values.

# COMMAND ----------

tup = ('Apple', 'Banana', 'Cherry')
tup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sets are unordered group of values

# COMMAND ----------

s = {'Apple', 'Banana'}
s

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' in Set s

# COMMAND ----------

s.add('Cherry')
s

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dicts are unordered key-value pairs (hash tables)

# COMMAND ----------

d = {'a': 'Apple', 'b':'Banana'}
d

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Add 'Cherry' with key 'c' in dict d

# COMMAND ----------

d["c"]='Cherry'
d

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operators and Functions

# COMMAND ----------

# TODO: Implement this function.
# returns the sum only when a and b are two-digit numbers; otherwise return None
def addTwoDigitNumbers(a, b):
    if a/10>=1 and b/10>=1:
        return a+b
    else:
        return None

# COMMAND ----------

assert(addTwoDigitNumbers(10, 20) == 30)

# COMMAND ----------

# Unit-testing addTwoDigitNumbers function
# assert only gives error when two items are NOT matched
assert(addTwoDigitNumbers(10, 20) == 30)
assert(addTwoDigitNumbers(0, 20) == None)
assert(addTwoDigitNumbers(0, 100) == None)
assert(addTwoDigitNumbers(20, 0) == None)
assert(addTwoDigitNumbers(100, 0) == None)

# COMMAND ----------

addTwoDigitNumbers(14,15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loop

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete the loop below

# COMMAND ----------

# TODO: For each item in tup (defined above), print the FIRST two letters using for loop
for i in tup:
    print(i[0:2])

# COMMAND ----------

# TODO: For each item in tup (defined above), print the SECOND LAST letter using while loop
for i in tup:
    print(i[-2:-1])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterator and Generator

# COMMAND ----------

# MAGIC %md
# MAGIC ### An object which allows a programmer to traverse through all the elements of a collection, regardless of its specific implementation.
# MAGIC ### An iterator is an object which implements the iterator protocol
# MAGIC ### The iterator protocol consists of two methods: __iter__() and __next__()

# COMMAND ----------

xi = iter([1, 2, 3])

# COMMAND ----------

print(xi)
print(xi.__next__())
print(next(xi))
print(xi.__next__())
# Will give an error because xi only has 3 elements. 
# print(next(xi))

# COMMAND ----------

def count(start=0):
    num = start
    while True:
        yield num
        num += 1

# COMMAND ----------

c = count(0)
type(c)

# COMMAND ----------

# TODO: Write a for loop that prints out count from 0 to 9
i=0
while i<10:
    print(next(c))
    i+=1
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assignment
# MAGIC Create a script that will read and parse the given files and remove duplicates using python, then write back into a single CSV and 8 smaller JSON files respectively. <br>
# MAGIC When two rows are duplicates, they have the same information but might have different separators/casing. <br>
# MAGIC For example: <br>
# MAGIC <ul>
# MAGIC   <li>“1234567890” instead of “123-456-7890” </li>
# MAGIC   <li>“JANE” instead of “Jane”</li>
# MAGIC   <li>“     Tom” instead of “Tom”</li>
# MAGIC </ul>
# MAGIC 
# MAGIC ### For this Assignment, DO NOT USE PANDAS

# COMMAND ----------

# TODO: Read people_1 and people_2
file_people1 = "/dbfs/FileStore/tables/people_1.txt"
file_people2 = "/dbfs/FileStore/tables/people_2.txt"
with open(file_people1, 'r') as f:
    lines1 = f.read()
    print(lines1)
with open(file_people2, 'r') as f:
    lines2 = f.read()
    print(lines2)

# COMMAND ----------

# TODO: Clean the data
import re
l1list=lines1.splitlines()
for i in range(1,len(l1list)):
    l1list[i]=l1list[i].lower()
    l1list[i]=re.sub("-","",l1list[i])
    l1list[i]=re.sub(" ","",l1list[i])
    l1list[i]=re.sub("no.","",l1list[i])
    l1list[i]=re.sub("#","",l1list[i])
l1listnod=set(l1list[1:])   

header=l1list[0]

l2list=lines2.splitlines()
for i in range(1,len(l2list)):
    l2list[i]=l2list[i].lower()
    l2list[i]=re.sub("-","",l2list[i])
    l2list[i]=re.sub(" ","",l2list[i])
    l2list[i]=re.sub("no.","",l2list[i])
    l2list[i]=re.sub("#","",l2list[i])
l2listnod=set(l2list[1:])   



# COMMAND ----------

header

# COMMAND ----------

len(l2listnod)

# COMMAND ----------

l1listnod=list(l1listnod)
l2listnod=list(l2listnod)

# COMMAND ----------

len(l2listnod)

# COMMAND ----------

# TODO: Output data as a csv file
import csv
file = open("pcombined.csv", "w")
writer = csv.writer(file)
writer.writerow(header.split())
for i in range(0,len(l2listnod)):
    writer.writerow(l1listnod[i].split())
    writer.writerow(l2listnod[i].split())

# COMMAND ----------

dbutils.fs.ls("pcombined.csv")

# COMMAND ----------

# TODO: Output data as 8 smaller JSON files

data={}
data['1']='name'




# COMMAND ----------

data

# COMMAND ----------

 # create a dictionary
data = {}
     
 # Open a csv reader called DictReader
with open('/dbfs/pcombined.csv', encoding='utf-8') as csvf:
    csvReader = csv.DictReader(csvf)
         
        # Convert each row into a dictionary
        # and add it to data
    for rows in csvReader:
             
            # Assuming a column named 'No' to
            # be the primary key
        key = rows['FirstName']
        data[key] = rows

# COMMAND ----------

data

# COMMAND ----------

print(csvf)

# COMMAND ----------

csvf

# COMMAND ----------

import csv
# create a dictionary
data = []
indexn=1
     
 # Open a csv reader called DictReader
with open('/dbfs/pcombined.csv', encoding='utf-8') as csvf:
    csvReader = csv.DictReader(csvf)
         
        # Convert each row into a dictionary
        # and add it to data
    for rows in csvReader:
        kk=rows
        data.append(kk)

# COMMAND ----------

print(data)

# COMMAND ----------

import json
with open('/dbfs/FileStore/tables/t1.json', 'w+') as d:
        json.dump(data, d, indent=4)

# COMMAND ----------

l=len(data)
lj=l//8+1
import json
lj

# COMMAND ----------

for i in range(0,8):
    ll=i
    start=12500*i
    end=start+12500
    with open('/dbfs/FileStore/tables/'+str(ll)+'.json', 'w+') as d:
        json.dump(data[start:end], d, indent=4)

# COMMAND ----------

display(spark.read.option('multiline','true').json('/FileStore/tables/t1.json'))

# COMMAND ----------

import pandas as pd
df = pd.read_json('/dbfs/FileStore/tables/t1.json')

# COMMAND ----------

display(df)

# COMMAND ----------

list=[]
list.append('kk')

# COMMAND ----------

list

# COMMAND ----------


