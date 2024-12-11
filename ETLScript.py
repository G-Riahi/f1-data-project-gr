from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

import json
import yaml
import os

#########################################################
# -------------------------Goal--------------------------
# Converting simple YAML dataset files to a single CSV
# file and creating an Apache Airflow ETL process
#########################################################


# Function to extract all keys from a dataset
# Param:  folderName (string)    -> The name of the folder that stores yaml dataset
# Output: keys (list of strings) -> A list of all keys in these datasets

def datasetKeys(folderName):
    max_length = 0
    keys = []
    folder_path = os.path.join('/home/floppabox/f1/f1db/src/data', folderName)
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, 'r') as file:
            data=yaml.safe_load(file)
            if max_length<=len(list(data.keys())):
                max_length = len(list(data.keys()))
                keys = list(data.keys())

    return keys


# Function to generate the dataset in the form of a list of dictionaries
# Param:  folderName (string)         -> The name of the folder that stores yaml dataset
# Output: data (list of dictionaries) -> The dataset

def yamlConv(folderName):
    keys = datasetKeys(folderName)
    data = []
    folder_path = os.path.join('/home/floppabox/f1/f1db/src/data', folderName)
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)

        with open(file_path, 'r') as file:
            content_circuits=yaml.safe_load(file)
            record ={}

            for key in keys:
                record[key]= content_circuits.get(key)

            data.append(record)
    
    return data


# Function to convert the YAML dataset to csv, with PySpark
# Param:  - folderName (string)                          -> The name of the folder that stores yaml dataset
#         - appName (string, by default = 'YAML to CSV') -> the SparkSession app name

def sparkDataset(folderName, appName='YAML to CSV'):
    spark = SparkSession.builder.appName(appName).getOrCreate()

    dataset= spark.createDataFrame(yamlConv(folderName)).select(datasetKeys(folderName))

    if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
        print('creaing csv_datasets folder')
        os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

    print('-'*20)

    if os.path.isdir(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folderName)):
        print(f'updating the {folderName} csv files')
        dataset.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folderName), header=True, mode='overwrite')
    else:
        print(f'creating the {folderName}csv files')
        dataset.write.csv(os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folderName), header=True)



#testing apache airflow (for now)

def simpleFunc():
    print('bruh')

with DAG('firstDAG', schedule_interval=None, start_date=datetime(2024,12,11), catchup=False) as dag:
    hello_task = PythonOperator(
        task_id = 'hello_task',
        python_callable=simpleFunc,
    )
        