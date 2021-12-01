import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql import types
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col
from pyspark.sql.functions import split
from pyspark.sql.functions import when
from pyspark.sql.functions import round, concat, lit


""" 
	Dropping un-necessary columns
	1. Column year and month can be retrived from flight date. Hence dropping them.
	2. Lookup for OP_Unique_Carrier column and replacing by meaning full name.
	3. Removing State Name Abbreviation from ORIGIN_CITY_NAME and DEST_CITY_NAME. Eg: Denver, CO => Denver.
	4. Dropping ORIGIN_STATE_ABR, DEST_STATE_ABR, ORIGIN_STATE_FIPS and DEST_STATE_FIPS.
	5. Dropping DEP_DELAY, ARR_DELAY, ARR_DELAY_NEW and DEP_DELAY_NEW because it can be calculate from other fields. 
	6. Converting those columns [CARRIER_DELAY | WEATHER_DELAY | NAS_DELAY | SECURITY_DELAY | LATE_AIRCRAFT_DELAY ] to [DELAY_CODE | DELAY_PERC | TOTAL_DELAY] 
	   for effectively storing the data.
"""

def main(inputs,inputs1):
	delay_df = spark.read.csv(inputs,inferSchema = True, header = True)
	l_country_lookup_df = spark.read.csv(inputs1,inferSchema = True, header = True)

	delay_df = delay_df.na.fill(value=0)

	drop_columns = ["YEAR","MONTH","ORIGIN_STATE_ABR", "DEST_STATE_ABR", "ORIGIN_STATE_FIPS","DEST_STATE_FIPS","DEP_DELAY","DEP_DELAY","ARR_DELAY","ARR_DELAY_NEW","DEP_DELAY_NEW","CANCELLED","CANCELLATION_CODE","_c38"]
	delay_df = delay_df.drop(*drop_columns)

	delay_df = delay_df.join(l_country_lookup_df,delay_df.OP_UNIQUE_CARRIER == l_country_lookup_df.Code,"left")

	delay_df = delay_df.drop("OP_UNIQUE_CARRIER","Code").withColumnRenamed("Description","OP_UNIQUE_CARRIER")

	delay_df = delay_df.withColumn("ORIGIN_CITY_NAME",split(delay_df['ORIGIN_CITY_NAME'], ',').getItem(0)).withColumn("DEST_CITY_NAME",split(delay_df['DEST_CITY_NAME'], ',').getItem(0)).withColumn("TOTAL_DELAY",col("CARRIER_DELAY")+col("WEATHER_DELAY")+col("NAS_DELAY")+col("SECURITY_DELAY")+col("LATE_AIRCRAFT_DELAY"))

	delay_df = delay_df.withColumn("CARRIER_DELAY_PREC",when(col("CARRIER_DELAY") > 0, round(col("CARRIER_DELAY")/col("TOTAL_DELAY"))).otherwise(col("CARRIER_DELAY"))).withColumn("WEATHER_DELAY_PREC",when(col("WEATHER_DELAY") > 0, round(col("WEATHER_DELAY")/col("TOTAL_DELAY"))).otherwise(col("WEATHER_DELAY"))).withColumn("NAS_DELAY_PREC",when(col("NAS_DELAY") > 0, round(col("NAS_DELAY")/col("TOTAL_DELAY"))).otherwise(col("NAS_DELAY"))).withColumn("SECURITY_DELAY_PREC",when(col("SECURITY_DELAY") > 0, round(col("SECURITY_DELAY")/col("TOTAL_DELAY"))).otherwise(col("SECURITY_DELAY"))).withColumn("LATE_AIRCRAFT_DELAY_PREC",when(col("LATE_AIRCRAFT_DELAY") > 0, round(col("LATE_AIRCRAFT_DELAY")/col("TOTAL_DELAY"))).otherwise(col("LATE_AIRCRAFT_DELAY")))

	delay_df = delay_df.withColumn("DELAY_PERC",concat(col("CARRIER_DELAY_PREC"),lit(","),col("WEATHER_DELAY_PREC"),lit(","),col("NAS_DELAY_PREC"),lit(","),col("SECURITY_DELAY_PREC"),lit(","),col("LATE_AIRCRAFT_DELAY_PREC")))

	delay_df = delay_df.withColumn("DELAY_CODE",lit("NO_DELAY"))

	delay_df = delay_df.withColumn("DELAY_CODE", when(col("CARRIER_DELAY_PREC")>0,concat(col("DELAY_CODE"),lit("CARRIER_DELAY,")))
		.when(col("WEATHER_DELAY_PREC")>0,concat(col("DELAY_CODE"),lit("WEATHER_DELAY,")))
		.when(col("NAS_DELAY_PREC")>0, concat(col("DELAY_CODE"),lit("NAS_DELAY,")))
		.when(col("SECURITY_DELAY_PREC")>0,concat(col("DELAY_CODE"),lit("SECURITY_DELAY,")))
		.when(col("LATE_AIRCRAFT_DELAY_PREC")>0,concat(col("DELAY_CODE"),lit("LATE_AIRCRAFT_DELAY,"))))

	print(delay_df.select('DELAY_CODE').distinct().collect())

	# delay_df.show()

if __name__ == '__main__':
	inputs = sys.argv[1] # "/Users/dhruv/Downloads/636076285_T_ONTIME_REPORTING 2.csv"
	inputs1 = sys.argv[2] # "/Users/dhruv/Downloads/L_UNIQUE_CARRIERS.csv"
	spark = SparkSession.builder.appName('Cleaning and Transforming Delay Dataset').getOrCreate()
	main(inputs,inputs1)


