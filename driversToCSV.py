import yaml
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
import json

#########################################################
# -------------------------Goal--------------------------
# Converting the drivers yaml files to a single CSV file
#########################################################

folder_path = "/home/floppabox/f1/f1db/src/data/drivers" #P.S.: I like to name my virtual machines with wierd names lol 

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

#print(f'there are {max_length} keys in drivers:')
#print(keys)

# Preparing lists of dictionaries for the extraced drivers and relationships data

drivers_data, drivers_relationships_data = [], []

# Extracting all drivers data

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'r') as file:
        content_drivers=yaml.safe_load(file)
        record ={}

        for key in keys:
            if key != 'familyRelationships':
                record[key]= content_drivers.get(key)

        drivers_data.append(record)

# Extracting all relations of drivers

for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, 'r') as file:
        content_drivers=yaml.safe_load(file)
        record ={}

        if 'familyRelationships' in content_drivers.keys():
            for rel in content_drivers['familyRelationships']:
                record['driverId']=content_drivers.get('id')
                record['relationId']=rel.get('driverId')
                record['type']=rel.get('type')

            drivers_relationships_data.append(record)


# Creating spark dataframes

drivers = spark.createDataFrame(drivers_data).select([x for x in keys if x != 'familyRelationships'])
drivers_relationships = spark.createDataFrame(drivers_relationships_data)

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
    drivers_relationships.write.csv('/home/floppabox/f1/f1-data-project-gr/csv_datasets/drivers_relationships', header=True)
