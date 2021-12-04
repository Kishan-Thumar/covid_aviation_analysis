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
import covid_data, delay_data, passengers_data
from pyspark.sql.functions import monotonically_increasing_id


# spark = SparkSession.builder.appName('main').getOrCreate()
# # Make sure we have Spark 3.0+
# assert spark.version >= '3.0' 
# spark.sparkContext.setLogLevel('WARN')

def main(spark, delay, carrier, passengers,service_class,flight_type):
    covid_df = covid_data.fetch_covid_data(spark)
    delay_df = delay_data.fetch_delay_data(spark,delay,carrier)
    passengers_df = passengers_data.fetch_passengers_data(spark,passengers,service_class,flight_type)
    #print(passengers_df.show())
    # Joining delay_df and passengers_df

    select_cols = [delay_df.OP_UNIQUE_CARRIER,delay_df.CARRIER_DELAY,delay_df.WEATHER_DELAY, delay_df.NAS_DELAY, delay_df.SECURITY_DELAY, delay_df.LATE_AIRCRAFT_DELAY,delay_df.ARR_DELAY,delay_df.DEP_DELAY,passengers_df.DEST_STATE_NM,passengers_df.ORIGIN_STATE_NM,passengers_df.YEAR,passengers_df.MONTH,passengers_df.UNIQUE_CARRIER_NAME,passengers_df.Passengers,passengers_df.Seats,passengers_df.Occupancy_Ratio,passengers_df.Freight,passengers_df.Mail,passengers_df.Distance,passengers_df.DATA_SOURCE,passengers_df.CARRIER_ORIGIN, passengers_df.DEST_STATE_ABR, passengers_df.ORIGIN_STATE_ABR]
    
    aviation_df = delay_df.join(passengers_df, (delay_df.YEAR == passengers_df.YEAR) & (delay_df.MONTH == passengers_df.MONTH) & (delay_df.ORIGIN_STATE_NM == passengers_df.ORIGIN_STATE_NM) & (delay_df.DEST_STATE_NM == passengers_df.DEST_STATE_NM) & (delay_df.OP_UNIQUE_CARRIER == passengers_df.UNIQUE_CARRIER)).select(*select_cols)
    
    #aviation_df.filter(col("MONTH") == "4").show()

    covid_aviation_origin_df = aviation_df.join(covid_df, (aviation_df.YEAR == covid_df.CovidYear) & (aviation_df.MONTH == covid_df.CovidMonth) & (aviation_df.ORIGIN_STATE_ABR == covid_df.state))

    #covid_aviation_origin_df.filter(col("MONTH") == "4").show()

    covid_aviation_origin_df = covid_aviation_origin_df.withColumnRenamed("Cases","OriginStateCases")\
    .withColumnRenamed("Deaths","OriginStateDeaths")\
    .withColumnRenamed("NewCases","OriginStateNewCases")\
    .withColumnRenamed("NewDeaths","OriginStateNewDeaths")\
    .withColumnRenamed("VaccAdmin","OriginStateVaccAdmin")

    #covid_aviation_origin_df = covid_aviation_origin_df.select("OriginStateCases","OriginStateDeaths","OriginStateNewCases","OriginStateNewDeaths","OriginStateVaccAdmin")

    covid_aviation_dest_df = aviation_df.join(covid_df, (aviation_df.YEAR == covid_df.CovidYear) & (aviation_df.MONTH == covid_df.CovidMonth) & (aviation_df.DEST_STATE_ABR == covid_df.state))

    covid_aviation_dest_df = covid_aviation_dest_df.withColumnRenamed("Cases","DestStateCases")\
    .withColumnRenamed("Deaths","DestStateDeaths")\
    .withColumnRenamed("NewCases","DestStateNewCases")\
    .withColumnRenamed("NewDeaths","DestStateNewDeaths")\
    .withColumnRenamed("VaccAdmin","DestStateVaccAdmin")

    covid_aviation_origin_df.createOrReplaceTempView("covid_aviation_origin_df")
    covid_aviation_dest_df.createOrReplaceTempView("covid_aviation_dest_df")

    join_df = spark.sql("""
                        SELECT OD.*, DD.DestStateCases, DD.DestStateDeaths,
                        DD.DestStateNewCases, DD.DestStateNewDeaths,DestStateVaccAdmin
                        FROM covid_aviation_origin_df AS OD
                        JOIN covid_aviation_dest_df AS DD
                        ON OD.OP_UNIQUE_CARRIER = DD.OP_UNIQUE_CARRIER
                        AND IFNULL(OD.CARRIER_DELAY,0) = IFNULL(DD.CARRIER_DELAY,0)
                        AND IFNULL(OD.WEATHER_DELAY,0) = IFNULL(DD.WEATHER_DELAY,0)
                        AND IFNULL(OD.NAS_DELAY,0) = IFNULL(DD.NAS_DELAY,0)
                        AND IFNULL(OD.SECURITY_DELAY,0) = IFNULL(DD.SECURITY_DELAY,0)
                        AND IFNULL(OD.LATE_AIRCRAFT_DELAY,0) = IFNULL(DD.LATE_AIRCRAFT_DELAY,0)
                        AND OD.ARR_DELAY = DD.ARR_DELAY
                        AND OD.DEP_DELAY = DD.DEP_DELAY
                        AND OD.DEST_STATE_NM = DD.DEST_STATE_NM
                        AND OD.ORIGIN_STATE_NM = DD.ORIGIN_STATE_NM
                        AND OD.YEAR = DD.YEAR
                        AND OD.MONTH = DD.MONTH
                        AND OD.UNIQUE_CARRIER_NAME = DD.UNIQUE_CARRIER_NAME
                        AND OD.Passengers = DD.Passengers
                        AND OD.Seats = DD.Seats
                        AND OD.Occupancy_Ratio = DD.Occupancy_Ratio
                        AND OD.Freight = DD.Freight
                        AND OD.Mail = DD.Mail
                        AND OD.Distance = DD.Distance
                        AND OD.DATA_SOURCE = DD.DATA_SOURCE
                        AND OD.CARRIER_ORIGIN = DD.CARRIER_ORIGIN
                        AND OD.DEST_STATE_ABR = DD.DEST_STATE_ABR
                        AND OD.ORIGIN_STATE_ABR = DD.ORIGIN_STATE_ABR
                        AND OD.CovidYear = DD.CovidYear
                        AND OD.CovidMonth = DD.CovidMonth
    """)

    # schema = types.StructType(covid_aviation_dest_df.schema.fields + covid_aviation_origin_df.schema.fields)
    # test_df = covid_aviation_dest_df.rdd.repartition('ORIGIN_STATE_NM').zip(covid_aviation_origin_df.rdd.repartition('ORIGIN_STATE_NM')).map(lambda x: x[0]+x[1])
    # final_df = spark.createDataFrame(test_df, schema)


    #test_df = covid_aviation_origin_df.join(covid_aviation_dest_df,covid_aviation_dest_df.row_id_e == covid_aviation_origin_df.row_id )

    #covid_aviation_dest_df.show()

    # print(covid_aviation_df.show())
    #aviation_df = aviation_df.drop(passengers_df["DEST_STATE_NM"], passengers_df["ORIGIN_STATE_NM"], passengers_df["MONTH"], passengers_df["YEAR"], passengers_df["UNIQUE_CARRIER_NAME"])
    # covid_aviation_origin_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("covid_or.csv")
    #covid_aviation_origin_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("origin.csv")
    #covid_aviation_dest_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("dest.csv")
    # join_df.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("final_df.csv")
    #print(aviation_df.columns)

if __name__ == '__main__':
    delay = sys.argv[1] # "/Users/himalyabachwani/Downloads/636076285_T_ONTIME_REPORTING/*.csv"
    carrier = sys.argv[2] # "/Users/himalyabachwani/Downloads/L_UNIQUE_CARRIERS.csv"
    passengers = sys.argv[3] # "/Users/himalyabachwani/Documents/Big_Data_732/project_data/849844085_T_T100_SEGMENT_ALL_CARRIER.csv"
    service_class = sys.argv[4] # "/Users/himalyabachwani/Documents/Big_Data_732/project_data/Service_class.csv"
    flight_type = sys.argv[5] # "/Users/himalyabachwani/Documents/Big_Data_732/project_data/Data_source.csv"
    spark = SparkSession.builder.appName('main').getOrCreate()
    main(spark, delay, carrier, passengers,service_class,flight_type)