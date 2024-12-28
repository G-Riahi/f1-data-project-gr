from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

#############################################################
# ---------------------------Goal----------------------------
# Converting the constructors yaml files to a single CSV file
#############################################################

# Getting all possible keys in the consructors data

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


# Preparing lists of dictionaries for the extraced constructors and rebranding data

def transformData(folderPath, keys):
    constructors_data, rebrands_data = [], []
    filesPaths = [os.path.join(folderPath, fileName) for fileName in os.listdir(folderPath)]
    id = 0

    for filePath in filesPaths:
        with open(filePath, 'r') as file:
            content_const=yaml.safe_load(file)
            record_constructor = {key: content_const[key] for key in keys if key != 'chronology'}
            constructors_data.append(record_constructor)

            de = None # 'de' is "from" in french, since I can't use "from" as a variable
            if 'chronology' in content_const.keys() and any(rebrand.get('constructorIdTo') == content_const['id'] for rebrand in rebrands_data)==False:
                for reb in content_const['chronology']:
                    record_rebrand = {
                        'consructorIdFrom' : de,
                        'yearFrom' : reb['yearFrom'],
                        'constructorIdTo' : reb['constructorId'],
                        'yearTo' : reb['yearTo']
                    }
                    de = reb['constructorId']
                    rebrands_data.append(record_rebrand)

    return constructors_data, rebrands_data


# Transforming datasets to CSV

def constructorsToSpark(folder_path="/home/floppabox/f1/f1db/src/data/constructors" , folder1='constructors', folder2='rebranding_history', appName='YAML to CSV'):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    keys = keys_list(folder_path)
    constructors_data, rebrands_data = transformData(folder_path, keys)
    
    constructors = spark.createDataFrame(constructors_data).select([x for x in keys if x != 'chronology'])
    rebrands = spark.createDataFrame(rebrands_data).select(['consructorIdFrom','constructorIdTo', 'yearFrom', 'yearTo'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    outputPath = '/home/floppabox/f1/f1-data-project-gr/csv_datasets'
    os.makedirs(outputPath, exist_ok=True)

    constructors.write.csv(os.path.join(outputPath, folder1), header=True, mode='overwrite')
    rebrands.write.csv(os.path.join(outputPath, folder2), header=True, mode='overwrite')