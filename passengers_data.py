import sys, re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import split

spark = SparkSession.builder.appName('passengers data').getOrCreate()
# Make sure we have Spark 3.0+
assert spark.version >= '3.0' 
spark.sparkContext.setLogLevel('WARN')

@functions.udf(returnType=types.StringType())
def processed_file_name(flight):
    fka_re = " fka | f/k/a "
    dba_re = " dba | d/b/a "
    if ("fka" in flight) or ("f/k/a" in flight):
        return re.split(fka_re, flight)[0]
    elif ("dba" in flight) or ("d/b/a" in flight):
        return re.split(dba_re, flight)[-1] 
    else:
        return flight
    
def fetch_passengers_data(passengers,service_class,data_source):    
    #path to respective files
    #passengers = '/Users/himalyabachwani/Documents/Big_Data_732/project_data/849844085_T_T100_SEGMENT_ALL_CARRIER.csv'
    #service_class = '/Users/himalyabachwani/Documents/Big_Data_732/project_data/Service_class.csv'
    #data_source = '/Users/himalyabachwani/Documents/Big_Data_732/project_data/Data_source.csv'

    #passengers data
    passengers_data = spark.read.csv(passengers, header=True, inferSchema= True).withColumn('UNIQUE_CARRIER_NAME',processed_file_name('UNIQUE_CARRIER_NAME'))

    #lookup tables
    service_class_data = spark.read.csv(service_class, header=True, inferSchema= True)
    data_source_data = spark.read.csv(data_source, header=True, inferSchema= True)

    processed_passengers_data = passengers_data.select('PAYLOAD','SEATS','PASSENGERS','FREIGHT','MAIL','DISTANCE','AIRLINE_ID','UNIQUE_CARRIER','UNIQUE_CARRIER_NAME','ORIGIN_AIRPORT_ID','ORIGIN','ORIGIN_STATE_ABR','ORIGIN_STATE_NM','DEST_AIRPORT_ID','DEST','DEST_STATE_ABR','DEST_STATE_NM','YEAR','MONTH','CLASS','DATA_SOURCE').withColumn('CARRIER_NAME',processed_file_name('UNIQUE_CARRIER_NAME'))

    processed_passengers_data.createOrReplaceTempView("processed_passengers_data")
    service_class_data.createOrReplaceTempView("service_class_data")
    data_source_data.createOrReplaceTempView("data_source_data")


    final_passengers_data = spark.sql("""
                            SELECT SUM(PD.PASSENGERS) AS Passengers, SUM(PD.SEATS) AS Seats,
                            AVG(PD.PASSENGERS / PD.SEATS) AS Occupancy_Ratio,
                            SUM(PD.FREIGHT) AS Freight, SUM(PD.MAIL) AS Mail,
                            AVG(PD.DISTANCE) AS Distance, PD.UNIQUE_CARRIER, PD.UNIQUE_CARRIER_NAME,
                            PD.ORIGIN_STATE_ABR, PD.ORIGIN_STATE_NM, PD.DEST_STATE_ABR, PD.DEST_STATE_NM,
                            PD.YEAR, PD.MONTH, PD.CLASS, PD.DATA_SOURCE,
                            SC.Description AS SERVICE_CLASS, DS.Description AS CARRIER_ORIGIN
                            FROM processed_passengers_data AS PD
                            JOIN service_class_data AS SC
                            ON PD.CLASS = SC.Code
                            JOIN data_source_data AS DS
                            ON PD.DATA_SOURCE = DS.Code
                            WHERE DS.Code IN ('DU','DF')
                            GROUP BY PD.UNIQUE_CARRIER, PD.UNIQUE_CARRIER_NAME,
                            PD.ORIGIN_STATE_ABR, PD.ORIGIN_STATE_NM, PD.DEST_STATE_ABR, PD.DEST_STATE_NM,
                            PD.YEAR, PD.MONTH, PD.CLASS, PD.DATA_SOURCE,
                            SC.Description, DS.Description
    """)
    return final_passengers_data