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

def shifts_def(start = 1111):

    # Creates shifts data. Creates 1-3 (random) shifts for each schedule that has been created.
    # Prerequisites: employees and employeeScedules data to be created in the warehouse. 
    # Inputs: start: the starting point for the index. default is 1111. NOTE: if there is data in the warehouse use the last index+1 as the start and switch the write line to the append option


    # TODO: add randomly some non-home-cost-center shifts
    # delta =spark.read.format('delta').table("d_costCenters")
    # costCenters = delta.toPandas()

    delta =spark.read.format('delta').table("d_employees")
    employees = delta.toPandas()

    delta =spark.read.format('delta').table("d_employeeScedules")
    employeeScedules = delta.toPandas()


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

        dfStart = employeeScedules.loc[employeeScedules.employeeScheduleID == employeeSceduleID, 'scheduleStartDatetime'].item()
        dfEnd = employeeScedules.loc[employeeScedules.employeeScheduleID == employeeSceduleID, 'scheduleEndDatetime'].item()

        # f = [(start + relativedelta(hours=i)) for i in range(start.hour, end.hour, tempSize)]
        
        siftStarsEnds = pd.date_range(dfStart, dfEnd, periods=tempSize*2).values.reshape((-1 , 2))
        
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

    start = start
    # dfIndex = range(start, start + len(shifts))


    shifts['employeeScheduleID'] = range(start, start + len(shifts))
    # shifts = shifts.set_index(dfIndex).rename(columns={'index': 'employeeScheduleID'})

    shifts_spark = spark.createDataFrame(shifts)


    # shifts_spark.write.mode("append").format("delta").saveAsTable("d_shifts")
    shifts_spark.write.mode("overwrite").format("delta").saveAsTable("d_shifts")

    return True

# COMMAND ----------

def f_shifts_def(): 

    # Creates calculation (fact) data based on shifts.
    # Prerequisites: shifts data to be created in the warehouse. 
    # Inputs: -


    delta =spark.read.format('delta').table("d_shifts")
    shifts = delta.toPandas()

    f_shifts = shifts.groupby('employeeSceduleID').agg(
                                            countOfShiftIDs=('employeeSceduleID', 'count'),
                                            totalShiftDuration=('shiftDuration', np.sum)).reset_index()

    f_shifts_spark = spark.createDataFrame(f_shifts)

    # f_shifts_spark.write.mode("append").format("delta").saveAsTable("f_shifts")
    f_shifts_spark.write.mode("overwrite").format("delta").saveAsTable("f_shifts")

    return True
