from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *



if __name__ == '__main__':

    # -------- LOADING -------------
    spark = SparkSession.builder.appName("paytm").getOrCreate()
    
    print('----start showing the previes of tables----')
    # 1. read data
    df_country = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
        .load("file:///home/hadoop/data/paytm/countrylist.csv")
    df_station = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
        .load("file:///home/hadoop/data/paytm/stationlist.csv")

    df_weather = spark.read.format("csv").option("header", "true").option("inferSchema", "true")\
        .load("file:///home/hadoop/data/paytm/data/2019/*.gz")

    # 2. fill the missing leading 0s for col 'STN---' and convert type for later joining
    df_weather = df_weather.withColumn('STN_V', format_string('%06d', 'STN---'))

    df_country.show(1)
    df_station.show(1)
    df_weather.show(1)

    # 3. join country data and station data
    df_cntry_stn = df_station.join(df_country, df_station['COUNTRY_ABBR'] == df_country['COUNTRY_ABBR'], how='left')\
        .drop(df_country['COUNTRY_ABBR'])
    df_cntry_stn.show(1)
    # 4. join country/station/weather together
    df_cntry_stn_wea = df_cntry_stn.join(df_weather, df_cntry_stn['STN_NO'] == df_weather['STN_V'], how='left')
    df_cntry_stn_wea.show(5)

    print('----end showing the previes of tables----')
    print()



    # -------- Q1 -------------
    # 1. check the length for 'YEARMODA' col, turn out to be all 8 digit or missing
    print('unique value of col YEARMODA: ', df_cntry_stn_wea.withColumn('len', F.length(df_cntry_stn_wea['YEARMODA'])).select('len').distinct().collect())
    print()
    # 2. convert 'YEARMODA' type to string and get year from the string
    df_cntry_stn_wea = df_cntry_stn_wea.withColumn('YEARMODA', df_cntry_stn_wea['YEARMODA'].cast(StringType()))
    df_cntry_stn_wea = df_cntry_stn_wea.withColumn('YEAR', substring('YEARMODA', 1, 4))
    # 3. filter out the records that miss 'TEMP' value
    df_cntry_stn_wea_q1 = df_cntry_stn_wea.filter(df_cntry_stn_wea['TEMP'] != 9999.9)
    df_cntry_stn_wea_q1 = df_cntry_stn_wea_q1.select('COUNTRY_FULL', 'TEMP').groupBy('COUNTRY_FULL').agg(F.mean('TEMP'))
    df_cntry_stn_wea_q1 = df_cntry_stn_wea_q1.orderBy(F.col('avg(TEMP)').desc())
    print('hottest country is ', df_cntry_stn_wea_q1.first().COUNTRY_FULL)


    # -------- Q2 -------------
    # 1. convert 'FRSHTT' to string type and fill missing leading 0s to make it back to valid 6-digit string
    df_cntry_stn_wea_q2 = df_cntry_stn_wea.withColumn('FRSHTT', format_string('%06d', 'FRSHTT'))
    # 2. get Tornado or Funnel Cloud indicator
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.withColumn('IND', substring('FRSHTT', 6, 1))
    # 3. select required columns in country/station level
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.select('COUNTRY_FULL', 'YEARMODA', 'IND')
    # 4. if any of the station in the country has Tornado or Funnel Cloud in a day, the country is marked in that day.
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.groupBy('COUNTRY_FULL', 'YEARMODA').agg(sum('IND'))
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.withColumn('IND', (df_cntry_stn_wea_q2['sum(IND)'] >= 1).cast('integer'))
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.withColumnRenamed('sum(IND)', 'sum_ind')
    # 5. sort the order by date, and drop null date rows
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.orderBy(F.col('COUNTRY_FULL').desc(), F.col('YEARMODA').asc())
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.na.drop(subset=['YEARMODA'])
    # 6. get consecutive count for 'IND'
    df_cntry_stn_wea_q2.createOrReplaceTempView('df_cntry_stn_wea_q2')
    df_cntry_stn_wea_q2 = spark.sql('select *, ROW_NUMBER() OVER (PARTITION BY COUNTRY_FULL ORDER BY YEARMODA) WEEK_SERIAL from df_cntry_stn_wea_q2')

    df_cntry_stn_wea_q2.createOrReplaceTempView('df_cntry_stn_wea_q2')
    step1_sql = \
        '''
        select *
                , MAX(case when ind=0 then week_serial end) OVER (PARTITION BY COUNTRY_FULL ORDER BY YEARMODA ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) b0 
        from df_cntry_stn_wea_q2
        '''
    df_cntry_stn_wea_q2 = spark.sql(step1_sql)

    df_cntry_stn_wea_q2.createOrReplaceTempView('df_cntry_stn_wea_q2')
    step2_sql = \
        '''
        select *
                , MIN(case when ind=0 then week_serial end) OVER (PARTITION BY COUNTRY_FULL ORDER BY YEARMODA ROWS BETWEEN 0 PRECEDING AND UNBOUNDED FOLLOWING) f0 
        from df_cntry_stn_wea_q2
        '''
    df_cntry_stn_wea_q2 = spark.sql(step2_sql)
    df_cntry_stn_wea_q2 = df_cntry_stn_wea_q2.withColumn('window_size', (F.col('f0') - F.col('b0') - 1))

    df_cntry_stn_wea_q2_answer = df_cntry_stn_wea_q2.groupBy('COUNTRY_FULL').agg(max('window_size')).withColumnRenamed('max(window_size)', 'max_window_size')
    df_cntry_stn_wea_q2_answer = df_cntry_stn_wea_q2_answer.orderBy(F.col('max_window_size').desc())
    print('country that had most consecutive days of tornadoes/funnel cloud is ', df_cntry_stn_wea_q2_answer.first().COUNTRY_FULL)



    # -------- Q3 -------------
    # 2. convert 'YEARMODA' type to string and get year
    df_cntry_stn_wea = df_cntry_stn_wea.withColumn('YEARMODA', df_cntry_stn_wea['YEARMODA'].cast(StringType()))
    df_cntry_stn_wea = df_cntry_stn_wea.withColumn('YEAR', substring('YEARMODA', 1, 4))
    # 3. filter out the records that miss 'WDSP' value
    df_cntry_stn_wea_q3 = df_cntry_stn_wea.filter(df_cntry_stn_wea['WDSP'] != 999.9)
    # 4. agg mean 'WDSP' for each country
    df_cntry_stn_wea_q3 = df_cntry_stn_wea_q3.select('COUNTRY_FULL', 'WDSP').groupBy('COUNTRY_FULL').agg(F.mean('WDSP'))
    df_cntry_stn_wea_q3 = df_cntry_stn_wea_q3.orderBy(F.col('avg(WDSP)').desc())
    # 4. get the 2nd highest country (2nd row of above df)
    second_index = 1
    outs = (df_cntry_stn_wea_q3.rdd.zipWithIndex()\
        .filter(lambda x: x[1] == second_index)\
        .map(lambda x: x[0])\
        .collect())

    print('country of second highest wind speed is ', outs[0]['COUNTRY_FULL'])


    spark.stop()