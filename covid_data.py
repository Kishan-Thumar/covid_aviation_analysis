import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from config import key
from pyspark.sql import SparkSession, functions, types
from urllib.request import urlopen
from pyspark import SparkFiles

spark = SparkSession.builder.appName('covid data').getOrCreate()
# Make sure we have Spark 3.0+
assert spark.version >= '3.0' 
spark.sparkContext.setLogLevel('WARN')

def fetch_covid_data(covid_api):
    #covid_api = 'https://api.covidactnow.org/v2/states.timeseries.csv?apiKey='
    spark.sparkContext.addFile(covid_api+key)
    covid_df = spark.read.csv(SparkFiles.get("states.timeseries.csv"), header = True, inferSchema = True)
    covid_data = covid_df.select('date','country','state','fips','`actuals.cases`','`actuals.deaths`','`actuals.contactTracers`','`actuals.newCases`','`actuals.newDeaths`','`metrics.vaccinationsCompletedRatio`','`actuals.vaccinesAdministered`','`cdcTransmissionLevel`')
    return covid_data
