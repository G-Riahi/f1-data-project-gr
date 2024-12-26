from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

import json
import yaml
import os

###########################################################
# --------------------------Goal---------------------------
# Converting the seasons entrants yam files into a CSV file
###########################################################

def extractAllData():
    return None