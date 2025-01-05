import yaml
import os
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, BooleanType

#spark = SparkSession.builder.appName("YAML to CSV").getOrCreate()

def extractTransferData(spark: SparkSession):

    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("entrantId", StringType(), True),
        StructField("constructorId", StringType(), True),
        StructField("driverId", StringType(), True),
        StructField("tookPart", BooleanType(), True),
        StructField("testDriver", BooleanType(), True)
    ])
    #year,entrant,constructor,driver,tookPart,TestDriver

    readDataset = spark.read.schema(schema).option("header", True).csv("/home/floppabox/f1/f1-data-project-gr/csv_datasets/driversAllYears")

    readDataset.createOrReplaceGlobalTempView("driversAllYears")

    #debut dataset (containing driver ID, constructor ID and the debut year)

    spark.sql("""SELECT MIN(year) as debut_year, driverId
                FROM global_temp.driversAllYears
                WHERE tookPart == True
                GROUP BY driverId""").createOrReplaceGlobalTempView("debut")
    
    debutDB = spark.sql("""SELECT db.driverId, dr.constructorId, db.debut_year
                        FROM global_temp.driversAllYears dr
                            INNER JOIN global_temp.debut db
                            ON db.debut_year == dr.year AND db.driverId == dr.driverId""")
    
    #retirement dataset (not acurate enough)
    
    spark.sql("""SELECT *
                FROM (
                    SELECT MAX(year) as retirement_year, driverId
                    FROM global_temp.driversAllYears
                    WHERE tookPart == True
                    GROUP BY driverId
                ) AS temp_ret
                WHERE retirement_year < (SELECT MAX(year) as maximum FROM global_temp.driversAllYears)""").createOrReplaceGlobalTempView("retirement")
    
    retirementDB = spark.sql("""SELECT db.driverId, dr.constructorId, db.retirement_year
                            FROM global_temp.driversAllYears dr
                                INNER JOIN global_temp.debut db
                                ON db.retirement_year == dr.year AND db.driverId == dr.driverId""")
    
    #transfer dataset
    
    transferDB = spark.sql("""
        SELECT TransferOut.driverId, const_out, transfer_out, const_in, transfer_in
        FROM (
            SELECT MAX(year) AS transfer_out, driverId, constructorId AS const_out
            FROM global_temp.driversAllYears
            WHERE tookPart = True
            GROUP BY driverId, constructorId
        ) AS TransferOut
        INNER JOIN (
            SELECT MIN(year) AS transfer_in, driverId, constructorId AS const_in
            FROM global_temp.driversAllYears
            WHERE tookPart = True
            GROUP BY driverId, constructorId
        ) AS TransIn
        ON TransferOut.driverId = TransIn.driverId 
        AND TransferOut.transfer_out = TransIn.transfer_in - 1
    """)

    transferDB.createOrReplaceGlobalTempView("transfer")

    #breaks dataset (for this project, a break is considered as a driver not driving in all grand pris' of at least one season
    # due to bein either a test_driver or leaving F1 temporarily)

    spark.sql("""
        SELECT Break.driverId, const_out, break_year, const_in, return_year, return_year - break_year as gap
        FROM (
            SELECT MAX(year) AS break_year, driverId, constructorId AS const_out
            FROM global_temp.driversAllYears
            WHERE tookPart = True
            GROUP BY driverId, constructorId
        ) AS Break
        INNER JOIN (
            SELECT MIN(year) AS return_year, driverId, constructorId AS const_in
            FROM global_temp.driversAllYears
            WHERE tookPart = True
            GROUP BY driverId, constructorId
        ) AS Return
        ON Break.driverId = Return.driverId 
        AND Break.break_year < Return.return_year - 1
        GROUP BY Break.driverId, const_out, break_year, const_in, return_year
    """).createOrReplaceGlobalTempView("gapBreak")

    spark.sql("""
        SELECT gb.*
        FROM global_temp.gapBreak gb
        LEFT ANTI JOIN global_temp.transfer t
        ON gb.driverId = t.driverId AND gb.const_in = t.const_in AND gb.return_year == t.transfer_in
    """).createOrReplaceGlobalTempView("filteredGap")

    spark.sql("""
        SELECT fg.*
        FROM global_temp.filteredGap fg 
            INNER JOIN (SELECT driverId, const_in, return_year, MIN(gap) AS act
                        FROM global_temp.filteredGap fg
                        GROUP BY driverId, const_in, return_year) AS ab
            ON ab.driverId == fg.driverId AND ab.const_in == fg.const_in AND ab.return_year == fg.return_year AND fg.gap == ab.act
    """).createOrReplaceGlobalTempView("filteredGap")

    breakDB = spark.sql("""
        SELECT fg.*
        FROM global_temp.filteredGap fg 
            INNER JOIN (SELECT driverId, const_out, break_year, MIN(gap) AS act
                        FROM global_temp.filteredGap fg
                        GROUP BY driverId, const_out, break_year) AS ab
            ON ab.driverId == fg.driverId AND ab.const_out == fg.const_out AND ab.break_year == fg.break_year AND fg.gap == ab.act
    """)

    return debutDB, retirementDB, transferDB, breakDB

def saveToCSV(spark: SparkSession):
    debutDB, retirementDB, transferDB, breakDB = extractTransferData(spark)
    
    debuts = spark.createDataFrame(debutDB).select(['driverId', 'constructorId', 'debut_year'])
    retirements = spark.createDataFrame(retirementDB).select(['driverId', 'constructorId', 'retirement_year'])
    transfers = spark.createDataFrame(transferDB).select(['driverId', 'const_out', 'transfer_out', 'const_in', 'transfer_in'])
    breaks = spark.createDataFrame(transferDB).select(['driverId', 'const_out', 'break_year', 'const_in', 'return_year'])

    # Creating CSV files if the csv_datasets folder exists (or also creating the folder)

    outputPath = '/home/floppabox/f1/f1-data-project-gr/csv_datasets/transferGraph'
    os.makedirs(outputPath, exist_ok=True)

    debuts.write.csv(os.path.join(outputPath, 'debuts'), header=True, mode='overwrite')
    retirements.write.csv(os.path.join(outputPath, 'retirements'), header=True, mode='overwrite')
    transfers.write.csv(os.path.join(outputPath, 'transfers'), header=True, mode='overwrite')
    breaks.write.csv(os.path.join(outputPath, 'breaks'), header=True, mode='overwrite')