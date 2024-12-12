from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

#########################################################
# -------------------------Goal--------------------------
# Converting the circuits yaml files to a single CSV file
#########################################################


# Getting all possible keys in the circuits data

def keys_list(folder_path):
    max_length = 0
    keys = []
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r') as file:
            data=yaml.safe_load(file)
            if max_length<=len(list(data.keys())):
                max_length = len(list(data.keys()))
                keys = list(data.keys())    
    return keys


# Preparing lists of dictionaries for the extraced circuits and previous names data

def transform_data(folder_path):

    circuits_data, previous_names_data = [], []
    keys = keys_list(folder_path)
    id = 0
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as file:
            content_circuits=yaml.safe_load(file)
            record ={}

            for key in keys:
                if key != 'previousNames':
                    record[key]= content_circuits.get(key)

            circuits_data.append(record)
        
            if isinstance(content_circuits['previousNames'], list):
                for name in content_circuits['previousNames']:
                    record ={}
                    record['id']=id
                    id=id+1
                    record['circuitId']=content_circuits['id']
                    record['previousName']=name
                    previous_names_data.append(record)

    return circuits_data, previous_names_data


# Transforming the dataset to CSV

def circuitsToSpark(folder_path="/home/floppabox/f1/f1db/src/data/circuits" , folder1='circuits', folder2='previous_circuits_names', appName='YAML to CSV'):

    circuits_data, previous_names_data = transform_data(folder_path)
    keys = keys_list(folder_path)
    spark = SparkSession.builder.appName(appName).getOrCreate()
    circuits = spark.createDataFrame(circuits_data).select([x for x in keys if x != 'previousNames'])
    previous_circuit_names = spark.createDataFrame(previous_names_data).select(['id','circuitId', 'previousName'])

    if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
        print('creaing csv_datasets folder')
        os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

    print('-'*20)

    if os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder1}') \
        and os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder2}'):
        print('updating the drivers and relationships csv files')
        circuits.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True, mode='overwrite')
        previous_circuit_names.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True, mode='overwrite')
    else:
        print('creating the drivers and relationships csv files')
        circuits.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True)
        previous_circuit_names.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True)