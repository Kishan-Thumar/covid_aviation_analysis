import sys, re
assert sys.version_info >= (3, 5)
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import split, when, lit, col

# UDF function to get appropriate carrier name
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
    
# Function to fetch data from S3, performing ETL and returning final_df.
def fetch_passengers_data(spark,passengers):
    # Fetching passenger data from S3 
    passengers_data = spark.read.csv(passengers, header=True, inferSchema= True)\
        .withColumn('UNIQUE_CARRIER_NAME',processed_file_name('UNIQUE_CARRIER_NAME'))\
        .withColumn('CARRIER_ORIGIN',when(col('DATA_SOURCE') == "DU", lit("Domestic Data, US Carriers Only"))\
        .when(col('DATA_SOURCE') == "DF", lit("Domestic Data, Foreign Carriers"))\
        .when(col('DATA_SOURCE') == "IF", lit("International Data, Foreign Carriers"))\
        .when(col('DATA_SOURCE') == "IU", lit("International Data, US Carriers Only")))

    # Selecting revelant columns from passengers_data
    processed_passengers_data = passengers_data.select('PAYLOAD','SEATS','PASSENGERS','FREIGHT','MAIL','DISTANCE','AIRLINE_ID','UNIQUE_CARRIER','UNIQUE_CARRIER_NAME','ORIGIN_AIRPORT_ID','ORIGIN','ORIGIN_STATE_ABR','ORIGIN_STATE_NM','DEST_AIRPORT_ID','DEST','DEST_STATE_ABR','DEST_STATE_NM','YEAR','MONTH','CLASS','DATA_SOURCE', 'CARRIER_ORIGIN')

    # Creating views 
    processed_passengers_data.createOrReplaceTempView("processed_passengers_data")
    
    # Performing aggregation for 
    final_passengers_data = spark.sql("""
                            SELECT SUM(PD.PASSENGERS) AS Passengers, SUM(PD.SEATS) AS Seats,
                            AVG(PD.PASSENGERS / PD.SEATS) AS Occupancy_Ratio,
                            SUM(PD.FREIGHT) AS Freight, SUM(PD.MAIL) AS Mail,
                            AVG(PD.DISTANCE) AS Distance, PD.UNIQUE_CARRIER, PD.UNIQUE_CARRIER_NAME,
                            PD.ORIGIN_STATE_ABR, PD.ORIGIN_STATE_NM, PD.DEST_STATE_ABR, PD.DEST_STATE_NM,
                            PD.YEAR, PD.MONTH, PD.DATA_SOURCE, PD.CARRIER_ORIGIN
                            FROM processed_passengers_data AS PD
                            WHERE PD.DATA_SOURCE IN ('DU','DF')
                            GROUP BY PD.UNIQUE_CARRIER, PD.UNIQUE_CARRIER_NAME,
                            PD.ORIGIN_STATE_ABR, PD.ORIGIN_STATE_NM, PD.DEST_STATE_ABR, PD.DEST_STATE_NM,
                            PD.YEAR, PD.MONTH, PD.DATA_SOURCE, PD.CARRIER_ORIGIN
    """)
    return final_passengers_data