from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from python_scripts.DriversToCSV import driversToSpark
from python_scripts.CircuitsToCSV import circuitsToSpark
from python_scripts.ConstructorsToCSV import constructorsToSpark

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import json
import yaml
import os

#########################################################
# -------------------------Goal--------------------------
# Converting simple YAML dataset files to a single CSV
# file and creating an Apache Airflow ETL process
#########################################################


spark = SparkSession.builder.appName('YAML to CSV').getOrCreate()


# Function to extract all keys and file names from a dataset
# Param:  folderName (string)           -> The name of the folder that stores yaml dataset
# Output: - keys (list of strings)      -> A list of all keys in these datasets
#         - fileNames (list of strings) -> A list of all file names in the dataset

'''def datasetKeys2(folderName):
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

    return keys'''


def datasetKeys(folderName):
    keysSeen = {}
    folderPath = os.path.join('/home/floppabox/f1/f1db/src/data', folderName)

    fileNames = [file for file in os.listdir(folderPath)]

    for fileName in fileNames:
        file_path = os.path.join(folderPath, fileName)
        with open(file_path, 'r') as file:
            data=yaml.safe_load(file)
            for key in data.keys():
                if key not in keysSeen:
                    keysSeen[key]=None

    return list(keysSeen.keys()), fileNames

# Function to generate the dataset in the form of a list of dictionaries
# Param:  folderName (string)         -> The name of the folder that stores yaml dataset
# Output: data (list of dictionaries) -> The dataset

def processFile(filePath, keys):
    with open(filePath, 'r') as file:
        content=yaml.safe_load(file)
        record ={key: content[key] for key in keys}

    return record

def yamlConv(folderName):
    keys, fileNames = datasetKeys(folderName)
    folderPath = os.path.join('/home/floppabox/f1/f1db/src/data', folderName)
    filePaths = [os.path.join(folderPath, fileName) for fileName in fileNames]

    with ThreadPoolExecutor() as executor:
        data = list(executor.map(lambda filePath: processFile(filePath, keys), filePaths))
    
    return data


# Function to convert the YAML dataset to csv, with PySpark
# Param:  - folderName (string)                          -> The name of the folder that stores yaml dataset
#         - appName (string, by default = 'YAML to CSV') -> the SparkSession app name

def sparkDataset(folderName):
    keys, _ = datasetKeys(folderName)
    dataset = spark.createDataFrame(yamlConv(folderName)).select(keys)
    outputDir = os.path.join('/home/floppabox/f1/f1-data-project-gr/csv_datasets', folderName)

    if not os.path.isdir('/home/floppabox/f1/f1-data-project-gr/csv_datasets'):
        print('creaing csv_datasets folder')
        os.makedirs('/home/floppabox/f1/f1-data-project-gr/csv_datasets')

    print('-'*20)

    if os.path.isdir(outputDir):
        print(f'updating the {folderName} csv files')
        dataset.write.csv(outputDir, header=True, mode='overwrite')
    else:
        print(f'creating the {folderName}csv files')
        dataset.write.csv(outputDir, header=True)



#FIrst DAG function

with DAG('transformationDAG', schedule_interval=None, start_date=datetime(2024,12,11), catchup=False) as dag:
    clone_update_dataset = BashOperator(
        task_id = 'clone_update_dataset',
        bash_command="sh /home/floppabox/f1/f1-data-project-gr/pull-dataset.sh "
    )

    with TaskGroup("transformYAMLtoCSV", tooltip="YAML transfomation group") as YAMLtoCSV:
        
        with TaskGroup("normalizingDatasets", tooltip="normalizing datasets with different structures") as normData:
            transform_drivers = PythonOperator(
                task_id = 'transform_drivers',
                python_callable = driversToSpark,
            )

            transform_circuits = PythonOperator(
                task_id = "transform_circuits",
                python_callable = circuitsToSpark,
            )

            transform_constructors = PythonOperator(
                task_id = "transform_constructors",
                python_callable = constructorsToSpark,
            )

            [transform_drivers, transform_circuits, transform_constructors]

        with TaskGroup("transformSimpleDatasets", tooltip="transforming datasets with a simple structure") as transformData:
            transform_grand_prix = PythonOperator(
                task_id = "transform_grand_prix",
                python_callable = sparkDataset,
                op_args=['grands-prix',],
            )

            transform_chassis = PythonOperator(
                task_id = "transform_chassis",
                python_callable = sparkDataset,
                op_args=['chassis',],
            )

            transform_continents = PythonOperator(
                task_id = "transform_continents",
                python_callable = sparkDataset,
                op_args=['continents',],
            )

            transform_countries = PythonOperator(
                task_id = "transform_countries",
                python_callable = sparkDataset,
                op_args=['countries',],
            )

            transform_eng_manu = PythonOperator(
                task_id = "transform_eng_manu",
                python_callable = sparkDataset,
                op_args=['engine-manufacturers',],
            )

            transform_engines = PythonOperator(
                task_id = "transform_engines",
                python_callable = sparkDataset,
                op_args=['engines',],
            )

            transform_entrants = PythonOperator(
                task_id = "transform_entrants",
                python_callable = sparkDataset,
                op_args=['entrants',],
            )

            transform_tyres = PythonOperator(
                task_id = "transform_tyres",
                python_callable = sparkDataset,
                op_args=['tyre-manufacturers',],
            )

            [transform_chassis, transform_continents, transform_countries, transform_eng_manu, transform_engines, transform_entrants, transform_tyres]


        [transformData, normData]


    clone_update_dataset >> YAMLtoCSV