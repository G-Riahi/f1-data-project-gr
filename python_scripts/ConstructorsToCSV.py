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

def transform_data(folder_path):

    keys = keys_list(folder_path)
    constructors_data, rebrands_data = [], []

    return constructors_data, rebrands_data


# Transforming datasets to CSV

def circuitsToSpark(folder_path="/home/floppabox/f1/f1db/src/data/constructors" , folder1='constructors', folder2='rebranding_history', appName='YAML to CSV'):