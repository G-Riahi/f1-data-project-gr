from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

#########################################################
# -------------------------Goal--------------------------
# Converting the drivers yaml files to a single CSV file
#########################################################

folder_path = "/home/floppabox/f1/f1db/src/data/drivers" #P.S.: I like to name my virtual machines with wierd names lol 

spark = SparkSession.builder.appName("YAML to CSV").getOrCreate()

# Getting all possible keys in the drivers data

def keys(folder_path):
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

# Preparing lists of dictionaries for the extraced drivers and relationships data



# Extracting all drivers data

'''for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'r') as file:
        content_drivers=yaml.safe_load(file)
        record ={}

        for key in keys:
            if key != 'familyRelationships':
                record[key]= content_drivers.get(key)

        drivers_data.append(record)'''

def transformData(folder_path):

    keys = keys(folder_path)
    drivers_data, drivers_relationships_data = [], []
    id = 0

    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as file:
            content_drivers=yaml.safe_load(file)
            record ={}

            for key in keys:
                if key != 'familyRelationships':
                    record[key]= content_drivers.get(key)

            drivers_data.append(record)

            content=yaml.safe_load(file)
            
            if 'familyRelationships' in content.keys():
                for rel in content['familyRelationships']:
                    record ={}
                    record['id']=id
                    id=id+1
                    record['driverId']=content.get('id')
                    record['relationId']=rel.get('driverId')
                    record['type']=rel.get('type')
                    drivers_relationships_data.append(record)

    
    return drivers_data, drivers_relationships_data

# Extracting all relations of drivers

'''id=0
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)
    with open(file_path, 'r') as file:
        content=yaml.safe_load(file)
        
        if 'familyRelationships' in content.keys():
            for rel in content['familyRelationships']:
                record ={}
                record['id']=id
                id=id+1
                record['driverId']=content.get('id')
                record['relationId']=rel.get('driverId')
                record['type']=rel.get('type')
                drivers_relationships_data.append(record)
'''

# Creating spark dataframes

'''drivers = spark.createDataFrame(drivers_data).select([x for x in keys if x != 'familyRelationships'])
drivers_relationships = spark.createDataFrame(drivers_relationships_data).select(['id','driverId', 'relationId', 'type'])

# Creating CSV files if the csv_datasets folder exists (or also creating the folder)

if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
    print('creaing csv_datasets folder')
    os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

print('-'*20)

if os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers') \
    and os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers_relationships'):
    print('updating the drivers and relationships csv files')
    drivers.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers', header=True, mode='overwrite')
    drivers_relationships.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers_relationships', header=True, mode='overwrite')
else:
    print('creating the drivers and relationships csv files')
    drivers.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers', header=True)
    drivers_relationships.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers_relationships', header=True)'''



def driversToSpark(folder_path="/home/floppabox/f1/f1db/src/data/drivers" , folder1='drivers', folder2='drivers_relationships'):

    drivers_data, drivers_relationships_data = transformData(folder_path)
    keys = keys(folder_path)
    drivers = spark.createDataFrame(drivers_data).select([x for x in keys if x != 'familyRelationships'])
    drivers_relationships = spark.createDataFrame(drivers_relationships_data).select(['id','driverId', 'relationId', 'type'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
        print('creaing csv_datasets folder')
        os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

    print('-'*20)

    if os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder1}') \
        and os.path.isdir(f'/home/floppabox/f1/f1-data-project-gr/csv_datasets/{folder2}'):
        print('updating the drivers and relationships csv files')
        drivers.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True, mode='overwrite')
        drivers_relationships.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True, mode='overwrite')
    else:
        print('creating the drivers and relationships csv files')
        drivers.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder1), header=True)
        drivers_relationships.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folder2), header=True)