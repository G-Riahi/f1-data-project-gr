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

def keys_list(folderPath):
    fileNames = [file for file in os.listdir(folderPath)]
    keysSeen = {}

    for fileName in fileNames:
        file_path = os.path.join(folderPath, fileName)
        with open(file_path, 'r') as file:
            data=yaml.safe_load(file)
            for key in data.keys():
                if key not in keysSeen:
                    keysSeen[key]=None

    return list(keysSeen.keys())


# Preparing lists of dictionaries for the extraced circuits and previous names data

def transform_data(folderPath, keys):
    circuits_data, previous_names_data = [], []
    filesPaths = [os.path.join(folderPath, fileName) for fileName in os.listdir(folderPath)]
    id = 0

    for filePath in os.listdir(filesPaths):
        with open(filePath, 'r') as file:
            content_circuits=yaml.safe_load(file)
            record_circuit = {key: content_circuits[key] for key in keys if key != 'previousNames'}

            circuits_data.append(record_circuit)
        
            if isinstance(content_circuits['previousNames'], list):
                for name in content_circuits['previousNames']:
                    record_names = {
                        'id' : id,
                        'circuitId' : content_circuits['id'],
                        'previousName' : name
                    }
                    id += 1
                    previous_names_data.append(record_names)

    return circuits_data, previous_names_data


# Transforming the dataset to CSV

def circuitsToSpark(folder_path="/home/floppabox/f1/f1db/src/data/circuits" , folder1='circuits', folder2='previous_circuits_names', appName='YAML to CSV'):

    circuits_data, previous_names_data = transform_data(folder_path)
    keys = keys_list(folder_path)
    spark = SparkSession.builder.appName(appName).getOrCreate()
    circuits = spark.createDataFrame(circuits_data).select([x for x in keys if x != 'previousNames'])
    previous_circuit_names = spark.createDataFrame(previous_names_data).select(['id','circuitId', 'previousName'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    outputPath = '/home/floppabox/f1/f1-data-project-gr/csv_datasets'
    os.makedirs(outputPath, exist_ok=True)

    circuits.write.csv(os.path.join(outputPath, folder1), header=True, mode='overwrite')
    previous_circuit_names.write.csv(os.path.join(outputPath, folder2), header=True, mode='overwrite')