from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

#########################################################
# -------------------------Goal--------------------------
# Converting the drivers yaml files to a single CSV file
#########################################################



# Getting all possible keys in the drivers data

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

# Extracting all drivers data

def transformData(folderPath, keys):
    drivers_data, drivers_relationships_data = [], []
    filesPaths = [os.path.join(folderPath, fileName) for fileName in os.listdir(folderPath)]
    id = 0

    for filePath in filesPaths:

        with open(filePath, 'r') as file:
            content_drivers=yaml.safe_load(file)

            record_driver ={key: content_drivers[key] for key in keys if key != 'familyRelationships'}
            drivers_data.append(record_driver)
            
            if 'familyRelationships' in content_drivers.keys():
                for rel in content_drivers['familyRelationships']:
                    record_relation = {
                        'id': id,
                        'driverId' : content_drivers.get('id'),
                        'relationId' : rel.get('driverId'),
                        'type' : rel.get('type')
                    }
                    id+=1
                    drivers_relationships_data.append(record_relation)

    
    return drivers_data, drivers_relationships_data


# Creating spark dataframes

def driversToSpark(folder_path="/home/floppabox/f1/f1db/src/data/drivers" , folder1='drivers', folder2='drivers_relationships', appName='YAML to CSV'):

    spark = SparkSession.builder.appName(appName).getOrCreate()
    keys = keys_list(folder_path)

    drivers_data, drivers_relationships_data = transformData(folder_path, keys)
    drivers = spark.createDataFrame(drivers_data).select([x for x in keys if x != 'familyRelationships'])
    drivers_relationships = spark.createDataFrame(drivers_relationships_data).select(['id','driverId', 'relationId', 'type'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    outputPath = '/home/floppabox/f1/f1-data-project-gr/csv_datasets'
    os.makedirs(outputPath, exist_ok=True)

    drivers.write.csv(os.path.join(outputPath, folder1), header=True, mode='overwrite')
    drivers_relationships.write.csv(os.path.join(outputPath, folder2), header=True, mode='overwrite')