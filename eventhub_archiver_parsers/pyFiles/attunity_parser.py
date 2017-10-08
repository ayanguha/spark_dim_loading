from pyspark import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from multiprocessing.dummy import Pool as ThreadPool
import json
from collections import OrderedDict,namedtuple
from pyspark.sql import Row
import sys,os
import utility

''' 
Objective:
1. Recursively go through Event Hub Capture path and create an Avro Dataframe using com.databricks:spark-avro_2.11:3.2.0 package
2. Unpack EH Capture Avro output as per following : https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-capture-overview
3. Extract Body (Actual EH msg) and cast it to String (Only MsgType = DT (Attunity Specific))
4. Extract distinct table names from all the messages
5. Read Configured table & column metadata from schema.json file 
5. Go through the list of tables found in data (ie in Step (4)) and filter out un-configured tables. This is to safeguard any failure in case any new table is introduced at source
6. For each table which is found in data and is configured in schema.json, table extraction & processing is done in parallel.
   Parallel processing is handled by Multiprocessing Threadpools. 
   Number of threads in a pool: 1-10. 
   Number of Pool: numberOfTables/10 + 1

7. table extraction & processing function:
    
   a) Extract Attunity Specific fields from Msg: data, beforedata, operation, changeSequence, timestamp, streamPosition, transactionId
   b) From Data, JSON Decoder is used to convert json string to python Ordered dictionary.
   c) Add Event Hub related audit fields to the dictionary.
   d) Add Attunity audit fields to the dictionary.
   e) Add derived field as per business logic to the dictionary. Please see respective function for more details.
   f) Convert the dict to a namedtuple and then to a Row object (in order to keep column order)
   g) Finally create a dataframe (using toDF) 
   h) Insert data frame to Hive (Append)
'''


      

def main():
    spark = utility.createSparkSession("Attunity EH Archiver to Data Lake")
    sliceDt = sys.argv[1]
    data_source = sys.argv[2]
    file_location = utility.getFileLocation(data_source,sliceDt)
    print file_location

    base = utility.getBase(spark,file_location)
    ub = utility.getunpackBody(spark,base)
    if ub.count() > 0: 
        # Attunity specific stuff
        tableList = getTableList(spark,ub)
        sch = getConfiguredTableList()
        for tnameRow in tableList:
            try:
                getunpackBodyData(spark,ub,tnameRow['tname'],getConfiguredTableSchemaDefinition(sch,tnameRow['tname']))
                print "Successful: %s" %(tnameRow['tname'])
            except:
                print "Failed: %s" %(tnameRow['tname'])
                raise
        
    else:
        print "Source file %s and all subdirectories are Empty" %(file_location)
    spark.stop()

def getPool(numberOfTables):
    poolFactor = 10
    return ThreadPool((numberOfTables/poolFactor)+1)

def getConfiguredTableList():
    #resources = os.path.join(os.path.dirname(os.path.abspath(__file__)),"resource","schema.json")
    resources = "schema.json"
    return json.loads(open(resources,"r").read())["feedList"]

def getConfiguredTableSchemaDefinition(schemaStructList,tableName):
    
    for k in schemaStructList:
       if k["feed"]["sourceTableName"].lower() == tableName.lower():
           return k["feed"]
    


def getunpackBodyDataWrapper(tup):
    sparkSession = tup[0]
    bodyDF = tup[1]
    tablename = tup[2]
    feedSchemaDefinition = tup[3]
    return getunpackBodyData(sparkSession,bodyDF,tablename, feedSchemaDefinition)



def getTableList(sparkSess,unpackBody):
    '''
    Identify tables which are present in data.
    @Attunity-Specific
    '''
    unpackBody.registerTempTable("unpackBodyTemp")
    tableList = sparkSess.sql("select distinct get_json_object(body,'$.message.headers.schema_table') tname from unpackBodyTemp").collect()
    print tableList
    return tableList


def prepFinalPayload(payload,row,isDeleted=False):
    payload.append(isDeleted)
    '''
    # Add All Audit columns
    @EH-Specific
    '''

    payload.append(row["SequenceNumber"])
    payload.append(row["Offset"])
    payload.append(row["EnqueuedTimeUtc"])
    '''
    # Add All Audit columns
    @Attunity-Specific
    '''
    payload.append(row["operation"])
    payload.append(row["changeSequence"])
    payload.append(row["timestamp"])
    payload.append(row["streamPosition"])
    payload.append(row["transactionId"])
    #r = namedtuple('tableschema', payload.keys())(**payload) 
    return payload


def extractDataJson(data,columnList):
    out = []
    d = json.loads(data)
    for k in columnList:
        out.append(d[k])

    return out

def applyBusinessLogic(businessDateField,columnList):
    '''
    A trick for closure to work. Required to pass additional arguements into a function which is also used in map API
    @Attunity-Specific 
    '''
    def _createFinalpayload(row):
        data = row["data"]
        beforeData = row["beforeData"]

        '''
        Decode Json but keep the column order. Beforedata may or may not be available, so try-catch it

        '''

        d = extractDataJson(data,columnList)
        
        try:
            bd = extractDataJson(beforeData,columnList)
        except:
            bd = {businessDateField : None}

        res = []

        '''
        1. IF Operation = "INSERT" THEN Load data with isDelete=False
        2. IF Operation = "UPDATE" AND BeforeData.START_DATE <> data.START_DATE THEN Load 
               a. 1 record with before data with isDelete=True so that it can cancel the one which got updated.
               b. 1 record with data with isDelete=False
        3. IF Operation = "UPDATE" AND BeforeData.START_DATE = data.START_DATE THEN Load data with isDelete=False
        4. IF Operation = "DELETE" THEN Load data with isDelete=True
        5. IF Operation = "REFRESH" THEN Delete entire table and load data with isDeleted=False. 
           
        (1), (3) and (5) are hanlded by default else clause below.

        '''
        if row["operation"] == "DELETE":
            pld = prepFinalPayload(d,row,isDeleted=True)
            res.append(pld) 
        elif businessDateField and row["operation"] == "UPDATE" and ( json.loads(data)[businessDateField] <>  json.loads(beforeData)[businessDateField]):
            pld = prepFinalPayload(d,row,isDeleted=False)
            bpld = prepFinalPayload(bd,row,isDeleted=True)
            res.append(pld)
            res.append(bpld)
        else:  ### row["operation"] == "REFRESH" or row["operation"] == "INSERT":
            pld = prepFinalPayload(d,row,isDeleted=False)
            res.append(pld)
        
                
        return res

    return _createFinalpayload




def tableSchemaType(feedSchemaDefinition):
    audit_fields = ["isDeleted","audit_eventhub_SequenceNumber","audit_eventhub_Offset","audit_eventhub_EnqueuedTimeUtc","audit_msg_operation","audit_msg_changeSequence","audit_msg_timestamp","audit_msg_streamPosition","audit_msg_transactionId"]
    s = StructType()
    for x in feedSchemaDefinition["schema"]:
        s.add(x["columnName"],StringType(),True)

    for x in audit_fields:
        s.add(x,StringType(),True)
    columnList = [x["columnName"] for x in feedSchemaDefinition["schema"]]
    if feedSchemaDefinition["isBusinessDateUpdatePossible"]:
        bussDate = [x["columnName"] for x in feedSchemaDefinition["schema"] if x["isBusinessDate"]][0]
    else:
        bussDate = None

        
    return (s,columnList,bussDate)


def getunpackBodyData(sparkSess,unpackBody,tablename, feedSchemaDefinition):

    '''
    Extract attunity specific values
    @Attunity-Specific
    '''

    schema, columnList, bussDate = tableSchemaType(feedSchemaDefinition)
    targetSourceImageTable = feedSchemaDefinition["targetSourceImageTable"]
    unpackBody.registerTempTable("unpackBodyTemp")
    runSql = '''select get_json_object(body,'$.message.data') data,
                       get_json_object(body,'$.message.beforeData') beforeData,
                       get_json_object(body,'$.message.headers.operation') operation ,
                       get_json_object(body,'$.message.headers.changeSequence') changeSequence ,
                       get_json_object(body,'$.message.headers.timestamp') timestamp ,
                       get_json_object(body,'$.message.headers.streamPosition') streamPosition ,
                       get_json_object(body,'$.message.headers.transactionId') transactionId,
                       SequenceNumber, Offset, EnqueuedTimeUtc
              from unpackBodyTemp 
             where get_json_object(body,'$.message.headers.schema_table') = '<table_name>'
             '''.replace('<table_name>',tablename)

    unpackBodyFullMsg = sparkSess.sql(runSql)
    unpackBodyFullMsg.show(truncate=False)
    
    '''
        Get the operation for the batch. 
    '''
    op = unpackBodyFullMsg.select(unpackBodyFullMsg["operation"]).take(1)[0].asDict()["operation"]
    
    unpackBodyData = unpackBodyFullMsg.rdd.flatMap(applyBusinessLogic(bussDate,columnList)).toDF(schema)
    unpackBodyData.show(truncate=False)
    '''
    IF Operation = "REFRESH" THEN Delete entire table and load data again

    '''
    print "Writing to: %s" %(targetSourceImageTable)
    if op == "REFRESH":
        unpackBodyData.write.insertInto(targetSourceImageTable,overwrite=True)
    else:
        unpackBodyData.write.insertInto(targetSourceImageTable,overwrite=False)

     
    


if __name__ == "__main__":
    main()




