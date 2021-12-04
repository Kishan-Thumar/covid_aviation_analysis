import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import config
from pyspark.sql import SparkSession, functions, types
from urllib.request import urlopen
from pyspark import SparkFiles

# spark = SparkSession.builder.appName('covid data').getOrCreate()
# # Make sure we have Spark 3.0+
# assert spark.version >= '3.0' 
# spark.sparkContext.setLogLevel('WARN')

def fetch_covid_data(spark):
    covid_api = 'https://api.covidactnow.org/v2/states.timeseries.csv?apiKey='
    spark.sparkContext.addFile(covid_api+config.key)
    covid_df = spark.read.csv(SparkFiles.get("states.timeseries.csv"), header = True, inferSchema = True)
    covid_data = covid_df.select('date','country','state','fips','`actuals.cases`','`actuals.deaths`','`actuals.contactTracers`','`actuals.newCases`','`actuals.newDeaths`','`metrics.vaccinationsCompletedRatio`','`actuals.vaccinesAdministered`','`cdcTransmissionLevel`')
    covid_data.createOrReplaceTempView("covid_data")
    covid_grouped = spark.sql("""
                            SELECT YEAR(`date`) AS CovidYear, MONTH(`date`) AS CovidMonth,
                            `state`, SUM(`actuals.cases`) AS Cases, SUM(`actuals.deaths`) AS Deaths,
                            SUM(`actuals.newCases`) AS NewCases,SUM(`actuals.newDeaths`) AS NewDeaths,
                            SUM(`actuals.vaccinesAdministered`) AS VaccAdmin
                            FROM covid_data
                            GROUP BY YEAR(`date`), MONTH(`date`), `state`
    """)
    return covid_grouped
