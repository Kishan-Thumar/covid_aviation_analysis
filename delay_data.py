import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import when
from pyspark.sql.functions import round, concat, lit
from pyspark.sql.functions import udf, array
import re
import datetime
from pyspark.sql.types import ArrayType, StringType, IntegerType



""" 
	Dropping un-necessary columns
	1. Column year and month can be retrived from flight date. Hence dropping them.
	2. Lookup for OP_Unique_Carrier column and replacing by meaning full name.
	3. Removing State Name Abbreviation from ORIGIN_CITY_NAME and DEST_CITY_NAME. Eg: Denver, CO => Denver.
	4. Dropping ORIGIN_STATE_ABR, DEST_STATE_ABR, ORIGIN_STATE_FIPS and DEST_STATE_FIPS.
	5. Dropping DEP_DELAY, ARR_DELAY, ARR_DELAY_NEW and DEP_DELAY_NEW because it can be calculate from other fields. 
	6. Converting those columns [CARRIER_DELAY (1) | WEATHER_DELAY (2) | NAS_DELAY (3) | SECURITY_DELAY (4) | LATE_AIRCRAFT_DELAY (5) ] to [DELAY_CODE | DELAY_PERC | TOTAL_DELAY] 
	   for effectively storing the data.
	7. 
"""
get_time = udf(lambda z,y: get_time_diff(z,y),StringType())

def get_time_diff(a,b):
    a = str(a)
    a = a.rjust(4,"0")
    b = str(b)
    b = b.rjust(4,"0")
    if a == "2400":
        a = "0000"
    if b == "2400":
        b = "0000"
    t1 = datetime.datetime.strptime(a, "%H%M")
    t2 = datetime.datetime.strptime(b, "%H%M")
    #if a > "1200" or (a < "1200" and b < "1200") or b > "12x00":
    #if (a >= "1200" and b >= "1200") or (a <= "1200" and b <= "1200") or (a > b):
    if (a.startswith("2") and b.startswith("00")):
        t = t2-t1
        r = t.total_seconds()
        return ((r+(24*3600))/60)*-1
    elif (a.startswith("00") and b.startswith("2")):
        t = t1-t2
        r = t.total_seconds()
        return ((r+(24*3600))/60)
    else:
        t = t1-t2
        return t.total_seconds()/60
    
#get_time_diff("2359","0001")
#get_time_diff("0001","2359")    

get_carrier_name = udf(lambda z: processed_file_name(z),StringType())

def processed_file_name(flight):
    fka_re = " fka | f/k/a "
    dba_re = " dba | d/b/a "
    if ("fka" in flight) or ("f/k/a" in flight):
        return re.split(fka_re, flight)[0]
    elif ("dba" in flight) or ("d/b/a" in flight):
        return re.split(dba_re, flight)[-1] 
    else:
    	return flight 

        
#delay_df.filter(delay_df["ARR_TEMP"]> "1400").count()
def fetch_delay_data(spark,delay,carrier):
    delay_df = spark.read.csv(delay,inferSchema = True, header = True).filter((col('CANCELLED')!="1") & (col('ARR_TIME').isNotNull()) & (col('DEP_TIME').isNotNull()) & (col('CRS_ARR_TIME').isNotNull()) & (col('CRS_DEP_TIME').isNotNull())).withColumn("DEP_TEMP",get_time(col("DEP_TIME"),col("CRS_DEP_TIME"))).withColumn("ARR_TEMP",get_time(col("ARR_TIME"),col("CRS_ARR_TIME")))
    #a = delay_df.filter((col("OP_UNIQUE_CARRIER") == "DL") & (col("ORIGIN_STATE_NM") == "Maryland") & (col("DEST_STATE_NM")=="Georgia")).show()
    #delay_df.filter((delay_df["CRS_ARR_TIME"] == "1706") & (delay_df["DEST_STATE_NM"] == "North Carolina") & (delay_df["DEP_TIME"] == "1457")).show()
    l_country_lookup_df = spark.read.csv(carrier,inferSchema = True, header = True)

    #delay_df = delay_df.na.fill(value=0)

    drop_columns = ["FL_DATE","ORIGIN_STATE_ABR", "DEST_STATE_ABR", "ORIGIN_STATE_FIPS","DEST_STATE_FIPS","DEP_DELAY","ARR_DELAY","ARR_DELAY_NEW","DEP_DELAY_NEW","CANCELLED","CANCELLATION_CODE","_c38"]
    delay_df = delay_df.drop(*drop_columns)

    delay_df = delay_df.join(l_country_lookup_df,delay_df.OP_UNIQUE_CARRIER == l_country_lookup_df.Code,"left")

    delay_df = delay_df.drop("Code").withColumnRenamed("Description","UNIQUE_CARRIER_NAME")

    delay_df = delay_df.withColumn("ORIGIN_CITY_NAME",split(delay_df['ORIGIN_CITY_NAME'],',').getItem(0)).withColumn("DEST_CITY_NAME",split(delay_df['DEST_CITY_NAME'], ',').getItem(0))

    group_by_cols = ["OP_UNIQUE_CARRIER","MONTH","YEAR","DEST_STATE_NM","ORIGIN_STATE_NM","UNIQUE_CARRIER_NAME"]

    select_cols = ["OP_UNIQUE_CARRIER","UNIQUE_CARRIER_NAME","MONTH","YEAR","CARRIER_DELAY","WEATHER_DELAY", "NAS_DELAY", "SECURITY_DELAY", "LATE_AIRCRAFT_DELAY","ARR_DELAY","DEP_DELAY","DEST_STATE_NM","ORIGIN_STATE_NM"]

    delay_df = delay_df.groupBy(*group_by_cols).agg(functions.avg(delay_df['CARRIER_DELAY']).alias("CARRIER_DELAY"),functions.avg(delay_df['WEATHER_DELAY']).alias("WEATHER_DELAY"),functions.avg(delay_df['NAS_DELAY']).alias("NAS_DELAY"),functions.avg(delay_df['SECURITY_DELAY']).alias("SECURITY_DELAY"),functions.avg(delay_df['LATE_AIRCRAFT_DELAY']).alias("LATE_AIRCRAFT_DELAY"),functions.avg(delay_df['DEP_TEMP']).alias("DEP_DELAY"),functions.avg(delay_df['ARR_TEMP']).alias("ARR_DELAY")).select(*select_cols)

    delay_df = delay_df.withColumn("UNIQUE_CARRIER_NAME",get_carrier_name("UNIQUE_CARRIER_NAME"))

    return delay_df

# if __name__ == '__main__':
# 	inputs = sys.argv[1] # "/Users/himalyabachwani/Downloads/636076285_T_ONTIME_REPORTING.csv"
# 	inputs1 = sys.argv[2] # "/Users/himalyabachwani/Downloads/L_UNIQUE_CARRIERS.csv"
# 	spark = SparkSession.builder.appName('Cleaning and Transforming Delay Dataset').getOrCreate()
# 	main(inputs,inputs1)