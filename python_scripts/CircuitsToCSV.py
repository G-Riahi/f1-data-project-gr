from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

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

# Extracting all previous names of circuits

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


# Creating spark dataframes

circuits = spark.createDataFrame(circuits_data).select([x for x in keys if x != 'previousNames'])
previous_circuit_names = spark.createDataFrame(previous_names_data).select(['id','circuitId', 'previousName'])

# Creating CSV files if the csv_datasets folder exists (or also creating the folder)

if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
    print('creaing csv_datasets folder')
    os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

print('-'*20)

if os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets/circuits') \
    and os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets/previous_circuits_names'):
    print('updating the circuits and previous names csv files')
    circuits.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/circuits', header=True, mode='overwrite')
    previous_circuit_names.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/previous_circuits_names', header=True, mode='overwrite')
else:
    print('creating the circuits and previous names csv files')
    circuits.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/circuits', header=True)
    previous_circuit_names.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/previous_circuits_names', header=True)