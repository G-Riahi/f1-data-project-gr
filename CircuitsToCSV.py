import yaml
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
import json

#########################################################
# -------------------------Goal--------------------------
# Converting the circuits yaml files to a single CSV file
#########################################################

folder_path = "/home/floppabox/f1/f1db/src/data/circuits" #P.S.: I like to name my virtual machines with wierd names lol 

spark = SparkSession.builder.appName("YAML to CSV").getOrCreate()

# Getting all possible keys in the drivers data

max_length = 0
keys = []
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as file:
        data=yaml.safe_load(file)
        if max_length<=len(list(data.keys())):
            max_length = len(list(data.keys()))
            keys = list(data.keys())

print(keys)

# Preparing lists of dictionaries for the extraced circuits and previous names data

circuits_data, previous_names_data = [], []

# Extracting all drivers data

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'r') as file:
        content_circuits=yaml.safe_load(file)
        record ={}

        for key in keys:
            if key != 'previousNames':
                record[key]= content_circuits.get(key)

        circuits_data.append(record)

# Extracting all relations of drivers

id=0
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as file:
        content=yaml.safe_load(file)
        
        if isinstance(content['previousNames'], list):
            for name in content['previousNames']:
                record ={}
                record['id']=id
                id=id+1
                record['circuitId']=content['id']
                record['previousName']=name
                previous_names_data.append(record)