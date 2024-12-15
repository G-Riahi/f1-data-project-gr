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


# Preparing lists of dictionaries for the extraced constructors and rebranding data

def transformData(folder_path):

    keys = keys_list(folder_path)
    constructors_data, rebrands_data = [], []

    id = 0
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as file:
            content_const=yaml.safe_load(file)
            record = {}

            for key in keys:
                if key != 'chronology':
                    record[key]= content_const.get(key)

            constructors_data.append(record)

            de = None
            if 'chronology' in content_const.keys() and any(rebrand.get('constructorIdTo') == content_const['id'] for rebrand in rebrands_data)==False:
                for reb in content_const['chronology']:
                    record = {}
                    record['consructorIdFrom'] = de
                    record['constructorIdTo'] = reb['constructorId']
                    de = reb['constructorId']
                    record['yearFrom'] = reb['yearFrom']
                    record['yearTo'] = reb['yearTo']
                    rebrands_data.append(record)


    return constructors_data, rebrands_data


# Transforming datasets to CSV

def constructorsToSpark(folder_path="/home/floppabox/f1/f1db/src/data/constructors" , folder1='constructors', folder2='rebranding_history', appName='YAML to CSV'):
    spark = SparkSession.builder.appName(appName).getOrCreate()

    constructors_data, rebrands_data = transformData(folder_path)
    keys = keys_list(folder_path)
    constructors = spark.createDataFrame(constructors_data).select([x for x in keys if x != 'chronology'])
    rebrands = spark.createDataFrame(rebrands_data).select(['consructorIdFrom','constructorIdTo', 'yearFrom', 'yearTo'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
        print('creaing csv_datasets folder')
        os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

    print('-'*20)

    if os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder1}') \
        and os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder2}'):
        print('updating the constructors and rebrands csv files')
        constructors.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True, mode='overwrite')
        rebrands.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True, mode='overwrite')
    else:
        print('creating the constructors and rebrands csv files')
        constructors.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True)
        rebrands.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True)