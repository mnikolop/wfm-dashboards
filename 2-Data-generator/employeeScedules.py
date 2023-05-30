# Databricks notebook source
# TODO 
# d	EmployeeSchedule	costCenterID - # this needs to be the employeecc 80% and a random ne 20%

# when adding new lines windoes will be from today till 2-3 months in the future. If date exists set edited to today()

# take into acount costCenterSchedulePublishedDate for end period of schedule publishing

# COMMAND ----------

import pandas as pd
import numpy as np
import random
import datetime
from datetime import date, timedelta



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

# d	EmployeeSchedule	employeeScheduleID
# d	EmployeeSchedule	employeeID
# d	EmployeeSchedule	employeeContractID
# d	EmployeeSchedule	costCenterID - for now this will only be the home cost center
# d	EmployeeSchedule	customerID
# d	EmployeeSchedule	serviceDeliveryID 

# d	EmployeeSchedule	scheduleStartDate
# d	EmployeeSchedule	scheduleStartDateTime
# d	EmployeeSchedule	beginningBreak1 - for now hardcoded to 1030
# d	EmployeeSchedule	endBreak1 - for now hardcoded to 1100
# d	EmployeeSchedule	beginningBreak2 - for now hardcoded to 1200
# d	EmployeeSchedule	endBreak2 - for now hardcoded to 1300
# d	EmployeeSchedule	beginningBreak3 - for now hardcoded to 1500
# d	EmployeeSchedule	endBreak3 - for now hardcoded to 1515
# d	EmployeeSchedule	beginningBreak4 - for now hardcoded to 2000
# d	EmployeeSchedule	endBreak4 - for now hardcoded to 2030
# d	EmployeeSchedule	breakType - since it is 4 breaks either have 4 breaktyes or none at all
# d	EmployeeSchedule	scheduleEndDate
# d	EmployeeSchedule	scheduleEndDateTime
# d	EmployeeSchedule	countAsScheduledHours - always true
# d	EmployeeSchedule	edited - 
# d	EmployeeSchedule	isAssigned - is some systems they are able to unasign schedules from people without assigning them to others. For now this is not set 
# d	EmployeeSchedule	isLeave - 20% true
# d	EmployeeSchedule	leaveReasonID - 5 different IDs 
# d	EmployeeScedules	isLeaveApproved - always true
# d	EmployeeScedules	leaveDayCount
# d EmployeeSchedule	isDeleted - 10% true
# d	EmployeeSchedule	originalEmployeeScheduleId - if isDeleted = true then copy employeeScheduleID else 0
# d	EmployeeSchedule	scheduleTypeID - 
# d	EmployeeSchedule	customerID 

# f	EmployeeSchedule	employeeScheduleID
# f	EmployeeScedules	scheduleDuration
# f	EmployeeScedules	breakDuration

# d	LeaveReasons	leaveReasonId - 


# COMMAND ----------

# merge employees and employee contracts and take a sample
employeesAndContracts = employees.merge(employeeContracts)
employeesAndContractsSample = employeesAndContracts.sample(50).reset_index(drop=True)

# COMMAND ----------

employeesAndContractsSample

# COMMAND ----------


l = []
for employeeID in employeesAndContractsSample.employeeID.drop_duplicates(keep='first').reset_index(drop=True):
    
    temp = pd.DataFrame()
    
    start = employeesAndContractsSample.loc[employeesAndContractsSample.employeeID == employeeID, 'contractStartDate'].dt.date.item()
    end = datetime.datetime.now() + pd.DateOffset(months=np.random.choice([0, 1, 2, 3]))
    # scheduleStartDateEnd = datetime.datetime.now() + pd.DateOffset(months=np.random.choice([0, 1, 2, 3]))
    bs = employeesAndContractsSample.loc[employeesAndContractsSample.employeeID == employeeID, 'baseSchedule'].item()
    #TODO take into acount costCenterSchedulePublishedDate for end period of schedule publishing


    # print(type(pd.date_range(start, end - timedelta(days=1),freq='d')) )
    temp['scheduleStartDate'] = pd.date_range(start, end - timedelta(days=1),freq='d')
    temp['scheduleStartDatetime'] = temp['scheduleStartDate'] + pd.DateOffset(hours=8)

    # print(bs[pd.date_range(start ,end - timedelta(days=1),freq='d').dayofweek] * 8)
    temp['scheduleDuration'] = bs[pd.date_range(start ,end - timedelta(days=1),freq='d').dayofweek] * 8

    # # print(pd.date_range(start ,end - timedelta(days=1),freq='d') + timedelta(days=1))
    temp['scheduleEndDatetime'] = temp['scheduleStartDatetime'] + temp["scheduleDuration"].apply(lambda y: pd.DateOffset(hours=y))
    temp['scheduleEndDate'] = temp['scheduleEndDatetime'].dt.date

    temp['employeeID'] = employeeID

    temp['costCenterID'] = employeesAndContractsSample.loc[employeesAndContractsSample.employeeID == employeeID, 'costCenterID'].item()

    temp['serviceDeliveryID'] = np.random.choice([1, 2, 3, 4, 5], size=len(temp))

    # temp['beginningBreak1'] = temp['scheduleStartDate'] + pd.DateOffset(hours=10.5)
    temp['beginningBreak1'] = temp['scheduleStartDate'] + pd.DateOffset(hours=10.5)
    temp['beginningBreak1'] = temp.beginningBreak1.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=10.5),  pd.Timestamp('NaT').to_pydatetime())

    # temp['endBreak1'] = temp['scheduleStartDate'] + pd.DateOffset(hours=11)
    temp['endBreak1'] = temp['scheduleStartDate'] + pd.DateOffset(hours=11)
    temp['endBreak1'] = temp.endBreak1.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=11),  pd.Timestamp('NaT').to_pydatetime())

    # temp['beginningBreak2'] = temp['scheduleStartDate'] + pd.DateOffset(hours=12)
    temp['beginningBreak2'] = temp['scheduleStartDate'] + pd.DateOffset(hours=12)
    temp['beginningBreak2'] = temp.beginningBreak2.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=12),  pd.Timestamp('NaT').to_pydatetime())

    # temp['endBreak2'] = temp['scheduleStartDate'] + pd.DateOffset(hours=13)
    temp['endBreak2'] = temp['scheduleStartDate'] + pd.DateOffset(hours=13)
    temp['endBreak2'] = temp.endBreak2.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=13),  pd.Timestamp('NaT').to_pydatetime())

    # temp['beginningBreak3'] = temp['scheduleStartDate'] + pd.DateOffset(hours=15)
    temp['beginningBreak3'] = temp['scheduleStartDate'] + pd.DateOffset(hours=15)
    temp['beginningBreak3'] = temp.beginningBreak3.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=15),  pd.Timestamp('NaT').to_pydatetime())

    # temp['endBreak3'] = temp['scheduleStartDate'] + pd.DateOffset(hours=15.25)
    temp['endBreak3'] = temp['scheduleStartDate'] + pd.DateOffset(hours=15.25)
    temp['endBreak3'] = temp.endBreak3.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=15.25),  pd.Timestamp('NaT').to_pydatetime())

    # temp['beginningBreak4'] = temp['scheduleStartDate'] + pd.DateOffset(hours=20)
    temp['beginningBreak4'] = temp['scheduleStartDate'] + pd.DateOffset(hours=20)
    temp['beginningBreak4'] = temp.beginningBreak4.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=20),  pd.Timestamp('NaT').to_pydatetime())

    # temp['endBreak4'] = temp['scheduleStartDate'] + pd.DateOffset(hours=20.5)
    temp['endBreak4'] = temp['scheduleStartDate'] + pd.DateOffset(hours=20.5)
    temp['endBreak4'] = temp.endBreak4.where(temp['scheduleEndDatetime'] >= temp['scheduleStartDate'] + pd.DateOffset(hours=20.5),  pd.Timestamp('NaT').to_pydatetime())

    temp['countAsScheduledHours'] = True

    temp['isLeave'] = np.random.choice([0, 1], size=len(temp), p=[0.8, 0.2])

    temp['leaveReasonID'] = np.random.choice([1, 2, 3, 4, 5], size=len(temp))

    temp['isLeaveApproved'] = True

    temp['leaveDayCount'] = np.random.choice(range(1, 300), size=len(temp))

    temp['isDeleted'] = np.random.choice([0, 1], size=len(temp), p=[0.9, 0.1])

    temp['customerID'] = np.random.choice(customers.customerID.to_list(), size=len(temp))


    # TODO # d	EmployeeSchedule	originalEmployeeScheduleId - if isDeleted = true then copy employeeScheduleID else 0

    # employeeScedules = employeeScedules.append(temp, ignore_index = True)

    l.append(temp)
employeeScedules = pd.concat(l,ignore_index = True)

start = 1111
# dfIndex = range(start, start + len(employeeScedules))


employeeScedules['employeeScheduleID'] = range(start, start + len(employeeScedules))
# employeeScedules = employeeScedules.set_index(dfIndex).rename(columns={'index': 'employeeScheduleID'})


# COMMAND ----------

employeeScedules

# COMMAND ----------

employeeScedules_spark = spark.createDataFrame(employeeScedules)


# COMMAND ----------

# employeeScedules_spark.write.mode("append").format("delta").saveAsTable("d_employeeScedules")
employeeScedules_spark.write.mode("overwrite").format("delta").saveAsTable("d_employeeScedules")


# COMMAND ----------

delta =spark.read.format('delta').table("d_employeeScedules")
employeeScedules = delta.toPandas()

# COMMAND ----------

f_employeeSchedule = pd.DataFrame()

f_employeeSchedule['employeeScheduleID'] = employeeScedules['employeeScheduleID']

break1Duration = (employeeScedules['endBreak1'] - employeeScedules['beginningBreak1']).astype('timedelta64[m]')
break2Duration = (employeeScedules['endBreak2'] - employeeScedules['beginningBreak2']).astype('timedelta64[m]')
break3Duration = (employeeScedules['endBreak3'] - employeeScedules['beginningBreak3']).astype('timedelta64[m]')
break4Duration = (employeeScedules['endBreak4'] - employeeScedules['beginningBreak4']).astype('timedelta64[m]')
f_employeeSchedule['breakDuration'] = break1Duration + break2Duration + break3Duration + break3Duration
f_employeeSchedule['breakDuration'] = f_employeeSchedule['breakDuration'].fillna(0)

f_employeeSchedule['scheduleDuration'] = (employeeScedules['scheduleEndDatetime'] - employeeScedules['scheduleStartDatetime']).astype('timedelta64[m]')

f_employeeSchedule['totalScheduleDuration'] = f_employeeSchedule['scheduleDuration'] - f_employeeSchedule['breakDuration']

# COMMAND ----------

f_employeeSchedule

# COMMAND ----------

f_employeeSchedule_spark = spark.createDataFrame(f_employeeSchedule)


# COMMAND ----------

# f_employeeSchedule_spark.write.mode("append").format("delta").saveAsTable("f_employeeSchedule")
f_employeeSchedule_spark.write.mode("overwrite").format("delta").saveAsTable("f_employeeSchedule")

