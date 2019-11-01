from pyspark.sql.functions import date_format
from pyspark.sql.functions import from_unixtime, unix_timestamp
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import time
import os,subprocess
import pyspark.sql.functions 
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import *
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType,DateType,DecimalType
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import when,udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import *
from pyspark.sql import Row
import logging

###Creating a Folder in the Local Path
dbutils.fs.mkdirs("file:/neon/dh/merin/config")

#%fs cp dbfs:/FileStore/tables/neon_dh_dev_jumeriah_config_details.json  file:/neon/dh/merin/config/neon_dh_dev_jumeriah_config_details.json

#%run "./utils/Utils"  $Config_File_Location="/neon/dh/merin/config/" $Config_File="neon_dh_dev_jumeriah_config_details.json"

filename1="/FileStore/tables/customer.csv" 
filename2="/FileStore/tables/reservation.csv"
filename3="/FileStore/tables/RESERVATION_STAT_DAILY_20190115.csv"

jh_bt_customer_file_read = spark.read.option("header","true").option("delimiter",",").format("csv").load(filename1)
jh_bt_customer_file_read = jh_bt_customer_file_read.distinct()
jh_bt_reservation_file_read = spark.read.option("header","true").option("delimiter",",").format("csv").load(filename2)
jh_bt_stats_file_read = spark.read.option("header","true").option("delimiter","^").format("csv").load(filename3)

jh_bt_stats_file_read.count()

jh_bt_customer_file_read.registerTempTable("jh_bt_customer_file_read")
jh_bt_reservation_file_read.registerTempTable("jh_bt_reservation_file_read")

import datetime

def check_date(col):
   if col:
      correctDate = None
      try:
        newDate = datetime.datetime.strptime(col, "%Y-%m-%d")
        correctDate = True
      except ValueError:
        correctDate = False
      return correctDate
   else:
      False

check_date_udf = udf(check_date, BooleanType())

from datetime import datetime as dt
def week_of_year(input_date,date_format="%Y-%m-%d"):
  x=(dt.strptime(input_date,date_format))
  y=x.strftime('%U')
  return (int(y)+1) 
    
week_of_year_udf = udf(week_of_year,IntegerType())

def create_date_col(jh_bt_customer_reservation_merge,RefereanceDateColumn):
  jh_bt_customer_reservation_merge1 = jh_bt_customer_reservation_merge.withColumn(RefereanceDateColumn,from_unixtime(unix_timestamp(col(RefereanceDateColumn), 'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm:ss'))
  #print(temp)
  jh_bt_customer_reservation_merge2 = jh_bt_customer_reservation_merge1.withColumn(RefereanceDateColumn + "_MONTH",month(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))
  jh_bt_customer_reservation_merge3 = jh_bt_customer_reservation_merge2.withColumn(RefereanceDateColumn + "_WEEK_OLD",weekofyear(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))
  jh_bt_customer_reservation_merge4 = jh_bt_customer_reservation_merge3.withColumn(RefereanceDateColumn + "_YEAR",year(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))
  jh_bt_customer_reservation_merge5 = jh_bt_customer_reservation_merge4.withColumn(RefereanceDateColumn + "_DAY",dayofmonth(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))
  jh_bt_customer_reservation_merge6 = jh_bt_customer_reservation_merge5.withColumn(RefereanceDateColumn + "_QUARTER",quarter(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))
  jh_bt_customer_reservation_merge7 = jh_bt_customer_reservation_merge6.withColumn(RefereanceDateColumn + "_WEEKDAY",dayofweek(jh_bt_customer_reservation_merge1[RefereanceDateColumn]))  
  jh_bt_customer_reservation_merge8 = jh_bt_customer_reservation_merge7.withColumn(RefereanceDateColumn + "_WEEKEND",when(jh_bt_customer_reservation_merge7[RefereanceDateColumn + "_WEEKDAY"] > 5,1).otherwise(0))  
    
  return jh_bt_customer_reservation_merge8

jh_bt_customer_reservation_merge = spark.sql("select * from jh_bt_reservation_file_read c left join jh_bt_customer_file_read r on c.name_id = r.name_id")

jh_bt_merge_date1 = create_date_col(jh_bt_customer_reservation_merge,"BUSINESS_DATE_CREATED")
jh_bt_merge_date2 = create_date_col(jh_bt_merge_date1,"BEGIN_DATE")
jh_bt_merge_date3 = create_date_col(jh_bt_merge_date2,"END_DATE")

jh_bt_merge_tn = jh_bt_merge_date3.withColumn("totalNights", datediff(to_date(jh_bt_merge_date3.END_DATE), to_date(jh_bt_merge_date3.BEGIN_DATE )))
jh_bt_merge_ltd = jh_bt_merge_tn.withColumn("leadTimeDays", datediff(to_date(jh_bt_merge_tn.BEGIN_DATE), to_date(jh_bt_merge_tn.BUSINESS_DATE_CREATED )))

jh_bt_merge_ti = jh_bt_merge_ltd.withColumn("total_individuals",jh_bt_merge_ltd.ADULTS+jh_bt_merge_ltd.CHILDREN)
jh_bt_merge_cd = jh_bt_merge_ti.withColumn("market_main_group",substring(jh_bt_merge_ti.MARKET_CODE,0,3))
jh_bt_merge_pc = jh_bt_merge_cd.withColumn("period_created",from_unixtime(unix_timestamp(jh_bt_merge_cd.BUSINESS_DATE_CREATED, 'yyyy-MM-dd HH:mm:ss'),'yyyyMM'))

jh_bt_merge_nia_temp = jh_bt_merge_pc.withColumn("nia_id", when(check_date_udf(from_unixtime(unix_timestamp(jh_bt_merge_pc.BUSINESS_DATE_CREATED, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')),lit('1')).otherwise(0))

jh_bt_merge_nia = jh_bt_merge_nia_temp.withColumn("name_id_age",when(jh_bt_merge_nia_temp.nia_id==1,round(year('begin_date')-year('business_date_created'))).otherwise("NULL"))

jh_bt_merge_dfnic_temp = jh_bt_merge_nia.withColumn("dfnic_id", when(check_date_udf(from_unixtime(unix_timestamp(jh_bt_merge_nia.INSERT_DATE, 'MM/dd/yyyy HH:mm'),'yyyy-MM-dd')),lit('1')).otherwise(0))

jh_bt_merge_dfnic = jh_bt_merge_dfnic_temp.withColumn("daysFromNameIdCreated",when(jh_bt_merge_dfnic_temp.dfnic_id==1,datediff(from_unixtime(unix_timestamp(jh_bt_merge_dfnic_temp.INSERT_DATE, 'MM/dd/yyyy HH:mm'),'yyyy-MM-dd HH:mm:ss'),"business_date_created")).otherwise("NULL"))

jh_bt_merge_ha = jh_bt_merge_dfnic.withColumn("hasallotment",when(col("BUSINESS_DATE_CREATED").isNotNull(),1).otherwise(0))

jh_bt_merge_nr = jh_bt_merge_ha.withColumn("num_row", row_number().over(Window.partitionBy("resv_name_id").orderBy("begin_date")))

jh_bt_merge_bdcw = jh_bt_merge_nr.withColumn("BUSINESS_DATE_CREATED_WEEK",week_of_year_udf(from_unixtime(unix_timestamp(jh_bt_merge_nr.BUSINESS_DATE_CREATED, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')))

jh_bt_merge_bdw = jh_bt_merge_bdcw.withColumn("BEGIN_DATE_WEEK",week_of_year_udf(from_unixtime(unix_timestamp(jh_bt_merge_bdcw.BEGIN_DATE, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')))

jh_bt_merge_edw = jh_bt_merge_bdw.withColumn("END_DATE_WEEK",week_of_year_udf(from_unixtime(unix_timestamp(jh_bt_merge_bdw.END_DATE, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')))

jh_bt_merge_edw.registerTempTable("jh_bt_merge_edw")

jh_bt_merge_final = jh_bt_merge_edw.select("RESORT","RESV_NAME_ID","c.NAME_ID","GROUP_ID","RATE_CODE","BUSINESS_DATE_CREATED","BUSINESS_DATE_CREATED_MONTH","BUSINESS_DATE_CREATED_QUARTER","BUSINESS_DATE_CREATED_WEEK","BUSINESS_DATE_CREATED_DAY","BUSINESS_DATE_CREATED_WEEKDAY","BUSINESS_DATE_CREATED_WEEKEND","PERIOD_CREATED","BEGIN_DATE","BEGIN_DATE_MONTH","BEGIN_DATE_QUARTER","BEGIN_DATE_WEEK","BEGIN_DATE_DAY","BEGIN_DATE_WEEKDAY","BEGIN_DATE_WEEKEND","END_DATE","END_DATE_MONTH","END_DATE_QUARTER","END_DATE_WEEK","END_DATE_DAY","END_DATE_WEEKDAY","END_DATE_WEEKEND","totalNights","leadTimeDays","hasAllotment","ADULTS","CHILDREN","total_individuals","EXTRA_BEDS","MULTIPLE_OCCUPANCY","SOURCE_CODE","MARKET_CODE","MARKET_MAIN_GROUP","CITY","COUNTRY","REGION_CODE","BOOKED_ROOM_CATEGORY","BOOKED_ROOM_CLASS","BOOKED_ROOM_LABEL","NAME_TAX_TYPE","PRINT_RATE_YN","CONFIRMATION_NO","RESV_STATUS","CREDIT_CARD_ID","FINANCIALLY_RESPONSIBLE_YN","PAYMENT_METHOD","POSTING_ALLOWED_YN","ARRIVAL_TRANSPORT_TYPE","ARRIVAL_STATION_CODE","ARRIVAL_CARRIER_CODE","ARRIVAL_TRANPORTATION_YN","DEPARTURE_TRANSPORT_TYPE","DEPARTURE_STATION_CODE","DEPARTURE_CARRIER_CODE","DEPARTURE_TRANSPORTATION_YN","MEMBERSHIP_ID","MEMBERSHIP_LEVEL","TURNDOWN_YN","CHANNEL","COMMISSION_PAYOUT_TO","PRE_CHARGING_YN","POST_CHARGING_YN","POST_CO_FLAG","hasExecutivePreferences","hasNewspaperPreferences","hasFloorPreferences","hasInterestsPreferences","hasViewPreferences","hasPreferences","NAME_ID_age","NAME_TYPE","INSERT_DATE","LAST_UPDATED_RESORT","RESORT_REGISTERED","daysFromNameIdCreated","hasBusinessAddress","hasHomeAddress","hasPostalAddress","hasAddress")
display(jh_bt_merge_final)

storage_account_name = "neondevstorage"
storage_account_access_key = "yDvmL0pq2c9cdvLdZGj5G9T6o/+hMbfEP5ps3YjPhUG0VEr+V0y+xQCLIVPsa5JtIQ7aI6VZASJr2XWZzySt5g=="

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net",storage_account_access_key)
jh_bt_merge_final.coalesce(1).write.format('com.databricks.spark.csv').options(delimiter=",",header=True).mode("append").save("wasbs://neon-dev-container-landing@neondevstorage.blob.core.windows.net/jumeriah_landing/jumeriah_usecase2_04022019")

jdbcHostname = "neon-dev-sqlserver.database.windows.net"
jdbcDatabase = "jmh_dm"
jdbcPort = 1433
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
Writeconfig = {
  "user" : "dhsqladmin",
  "password" : "DXB@2020",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

jh_bt_merge_final.write.jdbc(url=jdbcUrl, table="dbo.customer_reservation", mode="append", properties=Writeconfig)