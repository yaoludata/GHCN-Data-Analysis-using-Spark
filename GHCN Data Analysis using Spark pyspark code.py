# Imports

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *


# Required to allow the file to be submitted and run using spark-submit instead
# of using pyspark interactively

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

# Processing
# Q2

# (a)
# Define schema for daily

schema_daily = StructType([
    StructField("ID", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("VALUE", IntegerType(), True),
    StructField("MEASUREMENT_FLAG", StringType(), True),
    StructField("QUALITY_FLAG", StringType(), True),
    StructField("SOURCE_FLAG", StringType(), True),
    StructField("OBSERVATION_TIME", StringType(), True),
])

# Define schema for stations

schema_stations = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEVATION", DoubleType(), True),
    StructField("STATE", StringType(), True),
    StructField("NAME", StringType(), True),
    StructField("GSN_FLAG", StringType(), True),
    StructField("HCN/CRN_FLAG", StringType(), True),
    StructField("WMO_ID", StringType(), True),
])

# Define schema for countries

schema_countries = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

# Define schema for states

schema_states = StructType([
    StructField("CODE", StringType(), True),
    StructField("NAME", StringType(), True),
])

# Define schema for inventory

schema_inventory = StructType([
    StructField("ID", StringType(), True),
    StructField("LATITUDE", DoubleType(), True),
    StructField("LONGITUDE", DoubleType(), True),
    StructField("ELEMENT", StringType(), True),
    StructField("FIRSTYEAR", IntegerType(), True),
    StructField("LASTYEAR", IntegerType(), True),
])

# Q2 
# (b)
# Load the first 1000 rows of hdfs:///data/ghcnd/daily/2017.csv.gz

daily = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
    .limit(1000)
)
daily.cache()
daily.show(10, False)

# (c)
# Load stations (fixed width text formatting)

stations_text = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/stations")
)

stations_text.show(10, False)

stations = stations_text.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_stations['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_stations['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_stations['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 6)).alias('ELEVATION').cast(schema_stations['ELEVATION'].dataType),
    F.trim(F.substring(F.col('value'), 39, 2)).alias('STATE').cast(schema_stations['STATE'].dataType),
    F.trim(F.substring(F.col('value'), 42, 30)).alias('NAME').cast(schema_stations['NAME'].dataType),
    F.trim(F.substring(F.col('value'), 73, 3)).alias('GSN_FLAG').cast(schema_stations['GSN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 77, 3)).alias('HCN/CRN_FLAG').cast(schema_stations['HCN/CRN_FLAG'].dataType),
    F.trim(F.substring(F.col('value'), 81, 5)).alias('WMO_ID').cast(schema_stations['WMO_ID'].dataType)
)

stations.show(10, False)
stations.count()

# Load states (fixed width text formatting)

states_text = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/states")
)

states_text.show(10, False)

states = states_text.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_states['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_states['NAME'].dataType)
)

states.show(10, False)
states.count()

# Load countries (fixed width text formatting)

countries_text = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/countries")
)

countries_text.show(10, False)

countries = countries_text.select(
    F.trim(F.substring(F.col('value'), 1, 2)).alias('CODE').cast(schema_countries['CODE'].dataType),
    F.trim(F.substring(F.col('value'), 4, 47)).alias('NAME').cast(schema_countries['NAME'].dataType)
)

countries.show(10, False)
countries.count()

# Load inventory (fixed width text formatting)

inventory_text = (
    spark.read.format("text")
    .load("hdfs:///data/ghcnd/inventory")
)
inventory_text.show(10, False)

inventory = inventory_text.select(
    F.trim(F.substring(F.col('value'), 1, 11)).alias('ID').cast(schema_inventory['ID'].dataType),
    F.trim(F.substring(F.col('value'), 13, 8)).alias('LATITUDE').cast(schema_inventory['LATITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 22, 9)).alias('LONGITUDE').cast(schema_inventory['LONGITUDE'].dataType),
    F.trim(F.substring(F.col('value'), 32, 4)).alias('ELEMENT').cast(schema_inventory['ELEMENT'].dataType),
    F.trim(F.substring(F.col('value'), 37, 4)).alias('FIRSTYEAR').cast(schema_inventory['FIRSTYEAR'].dataType),
    F.trim(F.substring(F.col('value'), 42, 4)).alias('LASTYEAR').cast(schema_inventory['LASTYEAR'].dataType),
)

inventory.show(10, False)
inventory.count()

# calculate the number of stations do not have a WMO ID

stations.filter(stations.WMO_ID == "").count()

# Q3

# (a)
# Extract the country code from each station code, store the output as a new column

stations_1 = stations.withColumn('COUNTRY_CODE', F.substring((stations.ID), 1, 2))
stations_1.show(10, False)

# (b)
# LEFT JOIN stations with countries
# rename

countries_1 = (countries
    .withColumnRenamed('NAME', 'COUNTRY_NAME')
    )

countries_1.show()

# LEFT JOIN

stations_1 = (stations_1
    .join(countries_1, stations_1.COUNTRY_CODE == countries_1.CODE, 'left')
    .drop('CODE')
    )

stations_1.show(10, False)

# (c)
# LEFT JOIN stations and states

stations_2 = (stations
    .join(states, stations.STATE == states.CODE, 'left')
    .withColumnRenamed('CODE', 'STATE_CODE')
    .withColumnRenamed('NAME', 'STATE_NAME')
    .drop('STATE')
    )

stations_2.show(10, False)

# (d)
# The first and last year that each station was active and collected any element at all

station_active_period = (inventory.groupby('ID')
    .agg(F.min('FIRSTYEAR').alias('EARLIEST_FIRSTYEAR_ACTIVE'),
    F.max('LASTYEAR').alias('LASTEST_LASTYEAR_ACTIVE')
    )
    )

station_active_period.show(10, False)

# How many different elements has each station collected overall

station_elements = (inventory
    .select('ID', 'ELEMENT')
    .groupby('ID')
    .agg({'ELEMENT':'count'})
    .select('ID', F.col('count(ELEMENT)').alias('ELEMENT_COUNT'))
    .dropDuplicates()
    )

station_elements.show(10, False)

# filter core elements

core = ['PRCP', 'SNOW', 'TMIN', 'SNWD', 'TMAX']

inventory_core_elements = inventory.filter(inventory.ELEMENT.isin(core))

# count the core elements

inventory_core_elements.groupby('ELEMENT').count().show()

# count the number of core elements and join station_elements and inventory_core_elements
station_core_elements = (inventory_core_elements
    .select('ID', 'ELEMENT')
    .dropDuplicates()
    .groupby('ID')
    .agg({'ELEMENT':'count'})
    .select('ID', F.col('count(ELEMENT)').alias('CORE_ELEMENT_COUNT'))
    .join(station_elements, on='ID', how='left')
    )

station_core_elements.show()

# count the number of other elements

station_core_other_elements = (station_core_elements
    .withColumn('OTHER_ELEMENT_COUNT',
        F.col('ELEMENT_COUNT')-F.col('CORE_ELEMENT_COUNT')
        )
    )

station_core_other_elements.show()

# count the stations which collect all elements  20224

station_core_elements.filter(station_core_elements.CORE_ELEMENT_COUNT == '5').count()

# count the stations which only collection precipitation 15970

(station_core_other_elements.filter(station_core_other_elements.ELEMENT_COUNT=='1')
    .join(inventory.filter(inventory.ELEMENT == 'PRCP'), on='ID', how='inner')
    .count()
    )

# find the element set for each station

station_elements_set = (inventory
    .groupby('ID')
    .agg(F.collect_set('ELEMENT').alias('ELEMENT_SET'))
    )

station_elements_set.show()

# join the element set column to all the output created in part(d)

station_elements_summary = (station_core_other_elements
    .join(station_elements_set, on='ID', how='left')
    .join(station_active_period, on='ID', how='left')
    .dropDuplicates()
    )

station_elements_summary.show(10, False)

# (e)
# LEFT JOIN stations and your output from part(d)

enriched_stations = (stations_1
    .join(station_elements_summary, on='ID', how='left')
    .withColumnRenamed('STATE', 'STATE_CODE')
    .dropDuplicates()
    )

enriched_stations.show(10, False)

# save it to my output directory and the format is parquet

outputpath = 'hdfs:///user/ylu60/output/ghcnd/'
enriched_stations.write.format('parquet').mode('overwrite').save(outputpath+'enriched_stations.parquet')

# (f)
# LEFT JOIN my 1000 rows subset of daily and my output from part(e)

daily_stations = (daily
    .join(enriched_stations, on='ID', how='left')
    .dropDuplicates()
    )

daily_stations.show(10, False)

# Are there any stations in my subset of daily that are not in stations at all

daily_stations.filter(F.isnull('ELEMENT_COUNT')).count()

# without using LEFT JOIN

daily.select(['ID']).dropDuplicates().subtract(stations.select(['ID'])).count()

# Analysis
# Q1
# (a)
# How many stations are there in total: 103656

enriched_stations.count()

# How many stations have been active in 2017: 37070

enriched_stations.filter((enriched_stations.EARLIEST_FIRSTYEAR_ACTIVE < 2017) & (enriched_stations.LASTEST_LASTYEAR_ACTIVE >= 2017)).count()

# Count stations in GSN_FLAG:  991

enriched_stations.filter(enriched_stations.GSN_FLAG=='GSN').count()

# Count stations in HCN: 1218

enriched_stations.filter(F.col('HCN/CRN_FLAG')=='HCN').count()

# Count stations in CRN: 230

enriched_stations.filter(F.col('HCN/CRN_FLAG')=='CRN').count()

# find out the stations which are in more than of these networks: 14

more_than_one = (((enriched_stations.GSN_FLAG=='GSN').cast('integer') + (F.col('HCN/CRN_FLAG')=='HCN').cast('integer') + (F.col('HCN/CRN_FLAG')=='CRN').cast('integer')) > 1)
enriched_stations.filter(more_than_one).count()

# (b)
# The number of stations in each country

countries_stations = (enriched_stations
    .select('ID', 'COUNTRY_CODE')
    .dropDuplicates()
    .groupby('COUNTRY_CODE')
    .agg({'ID':'count'})
    .select('COUNTRY_CODE', F.col('count(ID)').alias('STATIONS_COUNT'))
    )
countries_stations.show()

# Store the output in countries using withColumnRenamed command

countries = (countries
    .join(countries_stations, countries.CODE == countries_stations.COUNTRY_CODE, how='left')
    .drop('COUNTRY_CODE')
    .withColumnRenamed('CODE', 'COUNTRY_CODE')
    .withColumnRenamed('NAME', 'COUNTRY_NAME')
    )

countries.show(10, False)

# save countries

outputpath = "hdfs:///user/ylu60/output/ghcnd/"
countries.write.format('parquet').mode("overwrite").save(outputpath+"countries.parquet")

# The number of stations in each states

states_stations = (enriched_stations
    .select('ID', 'STATE_CODE')
    .dropDuplicates()
    .groupby('STATE_CODE')
    .agg({'ID':'count'})
    .select('STATE_CODE', F.col('count(ID)').alias('STATIONS_COUNT'))
    )
states_stations.show()

# Store the output in states using withColumnRenamed command

states = (states
    .join(states_stations, states.CODE == states_stations.STATE_CODE, how='inner')
    .drop('CODE')
    .withColumnRenamed('NAME', 'STATES_NAME')
    )

states.show(10, False)

# save states

outputpath = "hdfs:///user/ylu60/output/ghcnd/"
states.write.format('parquet').mode("overwrite").save(outputpath+"states.parquet")

# (c)
# How many stations are there in the southern hemishpere:  25337

stations.filter(stations.LATITUDE < 0).count()

# How many stations are there in the rerritories of US: 57227

us_countries = countries.filter(countries.COUNTRY_NAME.like('%United States%')) # filter US with regular expression in countries
code_US = us_countries.rdd.map(lambda x:x[0]).collect()
code_US
countries.filter(countries.COUNTRY_CODE.isin(code_US)).show()

# Q2
# (a)
# writing a function to computes the geographical distance between two stations using latitude and longitude as argument

from math import radians, cos, sin, asin, sqrt
'''import math'''
def geodistance(lng1,lat1,lng2,lat2):
    '''compute the distance bewteen 2 stations by using latitude and longitude
       the unit of output is KM
    '''
    # approximate radius of earth in km
    r = 6371
    # Convert latitude and longitude to radians
    lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)])
    dlon = lng2 - lng1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    distance = 2 * asin(sqrt(a)) * r *1000
    distance = round(distance/1000,3)
    return distance

# Import

from pyspark.sql.functions import udf, col
geodistance_udf = F.udf(geodistance, DoubleType())

# (b)
# apply the function above to compute the pairwise distance between all stations in New Zealand
# creat a table for NZ's stations

NZ_stations = (enriched_stations
    .filter(enriched_stations.COUNTRY_CODE=='NZ')
    .select(['ID', 'LATITUDE', 'LONGITUDE'])
    )

NZ_stations.show()

NZ_stations_1 = (NZ_stations
    .withColumnRenamed('ID', 'ID1')
    .withColumnRenamed('LONGITUDE', 'LONGITUDE1')
    .withColumnRenamed('LATITUDE', 'LATITUDE1')
    )

NZ_stations_1.show()

NZ_stations_2 = (NZ_stations
    .withColumnRenamed('ID', 'ID2')
    .withColumnRenamed('LONGITUDE', 'LONGITUDE2')
    .withColumnRenamed('LATITUDE', 'LATITUDE2')
    )

NZ_stations_2.show()

# cross join NZ_stations_1 and NZ_stations_2

NZ_stations_12 = (NZ_stations_1
    .crossJoin(NZ_stations_2)
    .filter(F.col('ID1') != F.col('ID2'))
    )

NZ_stations_12.show()

# comput the geographical distance between 2 stations

NZ_stations_distance = (NZ_stations_12
    .withColumn('DISTANCE',
        geodistance_udf('LATITUDE1', 'LONGITUDE1', 'LATITUDE2', 'LONGITUDE2')
        )
    .dropDuplicates(['DISTANCE'])
    .sort('DISTANCE') #sort the distance to find  the closet stations in New Zealand
    )

NZ_stations_distance.show()

# save the result to my output directory

outputpath = 'hdfs:///user/ylu60/output/ghcnd/'
NZ_stations_distance.write.format('parquet').mode('overwrite').save(outputpath+'NZ_stations_distance.parquet')

# Q3
# (b)
# load and count the number of rows in daily for years 2010: 36946080

daily_2010 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2010.csv.gz")
)

daily_2010.count()

# load and count the number of rows in daily for years 2017: 21904999

daily_2017 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/2017.csv.gz")
)

daily_2017.count()

# (c)
# load and count the number of rows in daily for years 2010 to year 2015: 207716098

daily_2010_2015 = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/201[0-5].csv.gz")
)

daily_2010_2015.count()

# Q4
# (a)
# count the number of rows in daily: 2624027105

daily_all = (
    spark.read.format("com.databricks.spark.csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .schema(schema_daily)
    .load("hdfs:///data/ghcnd/daily/*.*")
)

daily_all.count()
daily_all.show(10, False)

# (b)
# filter daily using filter to obtain the subset of observations containing the five core elements

core = ['PRCP', 'SNOW', 'TMIN', 'SNWD', 'TMAX']
core_element_daily = (daily_all
    .filter(daily_all.ELEMENT.isin(core))
    )
    
core_element_daily.show()

# count the number of observations are there for eack of the five core elements which element has the most observation

core_element_daily_count = core_element_daily.groupby('ELEMENT').count().show()

# (c)
# how many observations of TMIN do not have corresponding observation of TMAX

# filter the data which only cotains TMIN and TMAX and count the total number which contains these two elements: 370056284

core_2 = ['TMIN', 'TMAX']
daily_TMAXTMIN = (core_element_daily
    .filter(core_element_daily.ELEMENT.isin(core_2))
    .groupby('ID', 'DATE')
    .agg(F.collect_set('ELEMENT').alias("TMAX/TMIN"))
)

daily_TMAXTMIN.show()
daily_TMAXTMIN.count()

# The number of stations collect TMAX in daily: 362528096

daily_TMAX = daily_TMAXTMIN.filter(F.array_contains("TMAX/TMIN", "TMAX"))
daily_TMAX.cache()
daily_TMAX.show()
daily_TMAX.count()

# The number of observations of TMIN do not have a corresponding observation of TMAX is 370056284 – 362528096 = 7528188

# how many different stations: 26625

daily_TMIN = (daily_TMAXTMIN
    .filter((F.array_contains('TMAX/TMIN', 'TMIN')) & (~ F.array_contains('TMAX/TMIN', 'TMAX')))
    .dropDuplicates(['ID'])
    )
    
daily_TMIN.count()

# which stations belong to the GSN,HCN or CRN: 2111

stations_TMIN = (daily_TMIN
	.join(stations, on='ID', how='left')
	.filter(
		(F.col('GSN_FLAG')!="")
		| (F.col('HCN/CRN_FLAG')!="")
		)
    .dropDuplicates(['ID'])
	)
    
stations_TMIN.count()

# (d)
# filter daily to obtain all observation of TMIN and TMAX for all stations in NZ

daily_nz = (enriched_stations
    .filter(F.col('COUNTRY_CODE')=="NZ"))
    
daily_nz.show()
    
daily_nz_TMAXTMIN = (daily_nz
    .join(daily_all, on='ID', how='inner')
    )

daily_nz_TMAXTMIN.show()

# count the number of observations: 447017.

daily_nz_TMAXTMIN = (daily_nz_TMAXTMIN
    .filter(daily_nz_TMAXTMIN.ELEMENT.isin(['TMAX','TMIN']))
    )
    
daily_nz_TMAXTMIN.show()
daily_nz_TMAXTMIN.count()

# save the result

outputpath = 'hdfs:///user/ylu60/output/ghcnd/'
daily_nz_TMAXTMIN.write.format('parquet').mode('overwrite').save(outputpath+'dailynz_TMAXTMIN.parquet')

# how many years are covered by the observations and find out the earlist date and latest date: 78

daily_nz_TMAXTMIN.groupby('COUNTRY_NAME').agg(F.min('DATE'), F.max('DATE')).show()

# extract the year in daily_nz_TMAXTMIN from DATE

daily_nz_TMAXTMIN_year = (daily_nz_TMAXTMIN
    .withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))
    )

daily_nz_TMAXTMIN_year.show(10, False)

daily_nz_TMAXTMIN_year.dropDuplicates(['YEAR']).count()

# write csv for plot

daily_nz_TMAXTMIN_year_write = (daily_nz_TMAXTMIN_year
    .select('ID', 'ELEMENT', "DATE", "YEAR", "VALUE")
    )
    
daily_nz_TMAXTMIN_year_write.show() 

# save the result as CSV 
outputpath = 'hdfs:///user/ylu60/output/ghcnd/'    
daily_nz_TMAXTMIN_year_write.repartition(1).write.format('csv').save(outputpath+'dailynz_temperaturemean.csv')

# (e)
# group the precipitation observations by year and country

daily_all = daily_all.withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))
daily_PRCP = (daily_all
    .filter(daily_all.ELEMENT.isin(['PRCP']))
    .join(enriched_stations, on="ID", how="left")
    .groupby(F.col('COUNTRY_CODE'), F.col('COUNTRY_NAME'), F.col('YEAR'))
    .agg(F.mean('VALUE').alias('mean_rainfall'))  # compute the average rainfull in each year for each country
    .sort(F.col('mean_rainfall'), ascending=False) # which country has the highest average rainfall in a single year across the entire dataset
    )

daily_PRCP.show()
daily_PRCP.cache()

# save this result and save this resultwrite csv for plot

outputpath = 'hdfs:///user/ylu60/output/ghcnd/'
daily_PRCP.repartition(1).write.format('csv').save(outputpath+'dailyrainfull.csv')

# Challenges
# Q1
# filter daily to obtain all observation of SNOW and SNWD for all stations in China

daily_ch = (enriched_stations
    .filter(F.col('COUNTRY_CODE')=="CH"))
    
daily_ch.show()
    
daily_ch_SNOWSNWD = (daily_ch
    .join(daily_all, on='ID', how='inner')
    )

daily_ch_SNOWSNWD.show()

daily_ch_SNOWSNWD = (daily_ch_SNOWSNWD
    .filter(daily_ch_SNOWSNWD.ELEMENT.isin(['SNOW','SNWD']))
    )
    
daily_ch_SNOWSNWD.show()
daily_ch_SNOWSNWD.count()

# count the total number of each element (SNOW and SNWD) for all stations in China

daily_ch_SNOWSNWD_count = daily_ch_SNOWSNWD.groupby('ELEMENT').count().show()

# find out the earlist date and latest date for each element (SNOW and SNWD) in China

daily_ch_SNOWSNWD.groupby('COUNTRY_NAME').agg(F.min('DATE'), F.max('DATE')).show()

# extract the year in daily_ch_SNOWSNWD from DATE

daily_ch_SNOWSNWD_year = (daily_ch_SNOWSNWD
    .withColumn('YEAR', F.trim(F.substring(F.col('DATE'), 1, 4)).cast(StringType()))
    )

daily_ch_SNOWSNWD_year.show(10, False)

daily_ch_SNOWSNWD_year.dropDuplicates(['YEAR']).count()

# write csv for plot

daily_ch_SNOWSNWD_year_write = (daily_ch_SNOWSNWD_year
    .select('ID', 'ELEMENT', "DATE", "YEAR", "VALUE")
    )
    
daily_ch_SNOWSNWD_year_write.show() 
 
outputpath = 'hdfs:///user/ylu60/output/ghcnd/'    
daily_ch_SNOWSNWD_year_write.repartition(1).write.format('csv').save(outputpath+'daily_ch_snow.csv')

# Q2

#select the column QUALITY_FLAG from daily_all

daily_qflag = (daily_all
    .select('QUALITY_FLAG')
    )
    
daily_qflag.show()

# count the number of observations are there for each of the five core elements which element has the most observation

daily_qflagy_count = daily_qflag.groupby('QUALITY_FLAG').count().show()



















