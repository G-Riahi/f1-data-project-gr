from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

###########################################################
# --------------------------Goal---------------------------
# Converting the seasons entrants yam files into a CSV file
###########################################################

def extractInfo(year, dataYaml, entrant):
    entryDrivers = []
    if 'constructorId' in dataYaml.keys():
        constructor = dataYaml['constructorId']
        entryDrivers.extend(drivers(year, dataYaml, entrant, constructor))
    else:
        for constructor in dataYaml['constructors']:
            entryDrivers.extend(drivers(year, constructor, entrant, constructor['constructorId']))
    return entryDrivers

def collectDriverInfo(year, entrant, constructor, dataYaml):
    data = {
            'year': int(year),
            'entrantId' : entrant,
            'constructorId' : constructor,
            'driverId' : dataYaml['driverId'],
            'tookPart': False if dataYaml['rounds'] == None else True,
            'TestDriver' : dataYaml.get('testDriver', False)
        }
    return data

def drivers(year, dataYaml, entrant, constructor):
    driversList = []
    if 'driverId' in dataYaml.keys():
        driversList.append(collectDriverInfo(year, entrant, constructor, dataYaml))
    else:
        for driver in dataYaml['drivers']:
            driversList.append(collectDriverInfo(year, entrant, constructor, driver))
    return driversList


def getAllDrivers(folderPath = "/home/floppabox/f1/f1db/src/data/seasons"):
    allDrivers = []
    years = [year for year in os.listdir(folderPath)]
    for year in sorted(years, key=int):
        file_path = os.path.join(folderPath, year, 'entrants.yml')
        with open(file_path, 'r') as file:
            data=yaml.safe_load(file)
            for entry in data:
                allDrivers.extend(extractInfo(year, entry, entry['entrantId']))
    return allDrivers


def entrantsDriversToSpark(folder='driversAllYears', appName='YAML to CSV'):

    circuits_data= getAllDrivers()
    spark = SparkSession.builder.appName(appName).getOrCreate()
    drivers = spark.createDataFrame(circuits_data).select(['year', 'entrantId', 'constructorId', 'driverID', 'tookPart', 'testDriver'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)
    outputPath = '/home/floppabox/f1/f1-data-project-gr/csv_datasets'
    os.makedirs(outputPath, exist_ok=True)

    drivers.write.csv(os.path.join(outputPath, folder), header=True, mode='overwrite')