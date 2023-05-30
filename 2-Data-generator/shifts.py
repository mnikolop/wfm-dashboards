# Databricks notebook source
import pandas as pd
import numpy as np
import random
import datetime
from datetime import date, timedelta
from dateutil.relativedelta import *



# COMMAND ----------

# MAGIC %sql
# MAGIC USE `hive_metastore`.`da_markella_nikolopoulou_themeli_rx1k_delp`;

# COMMAND ----------

delta =spark.read.format('delta').table("d_costCenters")
costCenters = delta.toPandas()


# COMMAND ----------

delta =spark.read.format('delta').table("d_employees")
employees = delta.toPandas()


# COMMAND ----------

delta =spark.read.format('delta').table("d_employeecontracts")
employeeContracts = delta.toPandas()


# COMMAND ----------

delta =spark.read.format('delta').table("d_localities")
localities = delta.toPandas()

# COMMAND ----------

delta =spark.read.format('delta').table("d_customers")
customers = delta.toPandas()

# COMMAND ----------

delta =spark.read.format('delta').table("d_employeeScedules")
employeeScedules = delta.toPandas()

# COMMAND ----------

# d	Shifts	shiftID
# d	Shifts	employeeSceduleID
# d	Shifts	shiftEndTime
# d	Shifts	shiftStartTime
# d	Shifts	causeForAttendance
# d	Shifts	serviceDeliveryID
# d	Shifts	serviceDeliveryKey
# d	Shifts	serviceDeliveryTitle



# f	Shifts	shiftID
# f	Shifts	shiftDuration
# f	Shifts	countOfShiftIDs



# COMMAND ----------

employeeScedules

# COMMAND ----------

size = 5000
dfIndex = range(0,size)

shifts = pd.DataFrame(index=dfIndex)

l = []
for employeeSceduleID in employeeScedules.employeeScheduleID.drop_duplicates(keep='first').reset_index(drop=True):
    
    tempSize = np.random.choice([1, 2, 3])
    # print(tempSize)
    dfIndex = range(0,tempSize)

    temp = pd.DataFrame(index=dfIndex)
    
    temp['employeeSceduleID'] = employeeSceduleID

    start = employeeScedules.loc[employeeScedules.employeeScheduleID == employeeSceduleID, 'scheduleStartDatetime'].item()
    end = employeeScedules.loc[employeeScedules.employeeScheduleID == employeeSceduleID, 'scheduleEndDatetime'].item()

    # f = [(start + relativedelta(hours=i)) for i in range(start.hour, end.hour, tempSize)]
    
    siftStarsEnds = pd.date_range(start, end, periods=tempSize*2).values.reshape((-1 , 2))
    
    # print(siftStarsEnds)

    temp['shiftStartTime'] = siftStarsEnds[:, 0]
    temp['shiftEndTime'] = siftStarsEnds[:, 1]

    temp['causeForAttendance'] = np.random.choice([1, 2, 3, 4, 5], size=len(temp))
    temp['serviceDeliveryID'] = np.random.choice([1, 2, 3, 4, 5], size=len(temp))
    
    temp['shiftDuration'] = (temp['shiftEndTime'] - temp['shiftStartTime']).astype('timedelta64[m]')
    
    # temp['isDeleted'] = np.random.choice([0, 1], size=len(temp), p=[0.9, 0.1])
    # print(temp)
    
    l.append(temp)
shifts = pd.concat(l,ignore_index = True)

start = 1111
# dfIndex = range(start, start + len(shifts))


shifts['employeeScheduleID'] = range(start, start + len(shifts))
# shifts = shifts.set_index(dfIndex).rename(columns={'index': 'employeeScheduleID'})


# COMMAND ----------

# shifts = pd.concat(l,ignore_index = True)

# start = 1111
# # dfIndex = range(start, start + len(shifts))


# shifts['employeeScheduleID'] = range(start, start + len(shifts))
# # shifts = shifts.set_index(dfIndex).rename(columns={'index': 'employeeScheduleID'})


# COMMAND ----------

shifts

# COMMAND ----------

shifts_spark = spark.createDataFrame(shifts)


# COMMAND ----------

# shifts_spark.write.mode("append").format("delta").saveAsTable("d_shifts")
shifts_spark.write.mode("overwrite").format("delta").saveAsTable("d_shifts")


# COMMAND ----------

f_shifts = shifts.groupby('employeeSceduleID').agg(
                                        countOfShiftIDs=('employeeSceduleID', 'count'),
                                        totalShiftDuration=('shiftDuration', np.sum)).reset_index()

# COMMAND ----------

f_shifts

# COMMAND ----------

f_shifts_spark = spark.createDataFrame(f_shifts)


# COMMAND ----------

# f_shifts_spark.write.mode("append").format("delta").saveAsTable("f_shifts")
f_shifts_spark.write.mode("overwrite").format("delta").saveAsTable("f_shifts")

