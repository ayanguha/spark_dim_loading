from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys,datetime,ConfigParser


'''
Objective:
1. Recursively go through Event Hub Capture path and create an Avro Dataframe using com.databricks:spark-avro_2.11:3.2.0 package
2. Unpack EH Capture Avro output as per following : https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview
3. Extract Body (Actual EH msg) and cast it to String

'''
def createSparkSession(appname="Bupa pyspark App"):
    spark = SparkSession \
           .builder \
           .appName(appname) \
           .enableHiveSupport() \
           .getOrCreate()
    
           #.config("spark.jars.packages","com.databricks:spark-avro_2.11:3.2.0") \
    spark._jsc.hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    return spark

def extractDT(sliceDT):
    # Expected Format: YYYY-MM-DD HH:MI:SS
    d = datetime.datetime.strptime(sliceDT,"%Y-%m-%d %H:%M:%S")
    return (d.strftime("%Y"),d.strftime("%m"),d.strftime("%d"),d.strftime("%H"))

def getTargetContainer(ds):
    conf = ConfigParser.ConfigParser()
    conf.read("eharchiver.conf")
    targetContainer = "wasb://<target_container>@<target_storage_account>" \
                      .replace("<target_container>",conf.get(ds,"target_container")) \
                      .replace("<target_storage_account>",conf.get(ds,"target_storage_account"))
    return targetContainer

def getTargetDateTimeDir(sliceDT):
    d = datetime.datetime.strptime(sliceDT,"%Y-%m-%d %H:%M:%S")
    return d.strftime("%Y/%m/%d/%H")

def getFileLocation(ds,sliceDT):
    conf = ConfigParser.ConfigParser()
    conf.read("eharchiver.conf")
    eventhub_archive_location = conf.get(ds,"eventhub_archive_location")

    sliceDt = sys.argv[1]
    yr,mth,dy,hr = extractDT(sliceDt)
    file_location = "<eventhub_archive_location>/*/<year>/<month>/<day>/<hour>/*/*" \
                    .replace("<eventhub_archive_location>",eventhub_archive_location) \
                    .replace("<year>",yr).replace("<month>",mth).replace("<day>",dy).replace("<hour>",hr)
    return file_location    

def getBase(sparkSess,hdfs_path):
    '''
    Unpack the avro format. Output will have EH Capture audit information AND a column called "Body" which holds actual msg data in binary
    '''
    df = sparkSess.read.format("com.databricks.spark.avro").load(hdfs_path)
    return df    

def getunpackBody(sparkSess,baseDF):
    '''
    Cast Binary "Body" field to a string
    '''
    bodyDF = baseDF.select(baseDF["body"].cast("string").alias("body"),baseDF["SequenceNumber"],baseDF["Offset"],baseDF["EnqueuedTimeUtc"] )
    #bodyDF.show(truncate=False)
    return bodyDF


