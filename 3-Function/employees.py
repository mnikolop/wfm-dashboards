# Databricks notebook source
import pandas as pd
import numpy as np
import random
import datetime


# COMMAND ----------

# MAGIC %sql
# MAGIC USE `hive_metastore`.`da_markella_nikolopoulou_themeli_rx1k_delp`;

# COMMAND ----------

# d	EmoployeeContracts	contractID
# d	EmoployeeContracts	costCenterID
# d	EmoployeeContracts	employmentClass 
# d	EmoployeeContracts	employmentClassTitle - 
# d	EmoployeeContracts	contractStartDate
# d	EmoployeeContracts	employmentEndDate 
# d	EmoployeeContracts	isActive
# d	EmoployeeContracts	baseSchedule <list of 7 percentages starting from Mon>
# d	EmoployeeContracts	totalWeeklyHours


# COMMAND ----------

def employeeContracts_def(sampleSize = 500, size = 500, start = 1111, employmentEndDateStartYears = 1):

    # Creates employeeContracts data.
    # Prerequisites: Localities data to be created in the warehouse.
    # Inputs: size: the number of cost centers to be breated.
            # start: the starting point for the index. default is 1111. NOTE: if there is data in the warehouse use the last index+1 as the start and switch the write line to the append option
            # employmentEndDateStartYears: How many years in the future should the contracts end. The range is defined as the number provided and the number provided + 4. Default is 1 and 1+4. The start of the contracts is definded as 10 years from the current year.



    delta =spark.read.format('delta').table("d_costCenters")
    costCenters = delta.toPandas()

    sampleSize = sampleSize
    dfIndex = range(0,sampleSize)
    columns=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    data = np.random.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1], size=(sampleSize*len(columns)))

    data = data.reshape(sampleSize, len(columns))

    baseSchedule = pd.DataFrame(columns=columns, index=dfIndex, data=data)


    size = size
    start = start
    dfIndex = range(start, start + size)

    employeeContracts = pd.DataFrame(index=dfIndex)

    # employeeContracts['employeeContractID'] = random.sample(range(1111,9999), size)
    employeeContracts = employeeContracts.reset_index().rename(columns={'index': 'employeeContractID'})

    employeeContracts['costCenterID']  = costCenters.costCenterID.sample(size, replace = True).reset_index(drop=True)

    employmentStartDateStart = datetime.datetime(year=datetime.datetime.now().year - 10, month=1, day=1)
    employmentStartDateEnd = datetime.datetime(year=datetime.datetime.now().year , month=1, day=1)
    employeeContracts['contractStartDate'] = np.random.random(size=(size,1)) * (employmentStartDateEnd - employmentStartDateStart) + employmentStartDateStart

    employmentEndDateStart = datetime.datetime(year=datetime.datetime.now().year + employmentEndDateStartYears, month=1, day=1)
    employmentEndDateEnd = datetime.datetime(year=datetime.datetime.now().year + employmentEndDateStartYears + 4, month=1, day=1)
    employeeContracts['contractEndDate'] = np.random.random(size=(size,1)) * (employmentEndDateEnd - employmentEndDateStart) + employmentEndDateStart

    employeeContracts['isActive'] = (employeeContracts['contractEndDate'] > (datetime.datetime.today())) & (employeeContracts['contractStartDate'] < datetime.datetime.today()) # because the date windows are set up so they don't need to check if they are properly made this will always be True.

    employeeContracts['baseSchedule'] = baseSchedule[columns].to_numpy().tolist()

    employeeContracts['totalWeeklyHours'] = np.sum((baseSchedule[columns]*8), axis=1)

    employeeContracts['employmentClass'] = np.random.choice([1, 2, 3, 4, 5], size=size)

    employeeContracts_spark = spark.createDataFrame(employeeContracts)


    # employeeContracts_spark.write.mode("append").format("delta").saveAsTable("d_employeeContracts")
    employeeContracts_spark.write.mode("overwrite").format("delta").saveAsTable("d_employeeContracts")

    return True

# COMMAND ----------

# d	Employees	employeeID
# d	Employees	address 
# d	Employees	birthDate -
# d	Employees	email - 
# d	Employees	fullName 
# d	Employees	gender
# d	Employees	isManager
# d	Employees	personalNumber - 
# d	Employees	phoneNumber - 

# f	Employees	employeeID
# f	Employees	countOfEmployeeID



# COMMAND ----------

def employees_def(size = 500, start = 1111):

    # Creates employees data.
    # Prerequisites: Localities and employee contracts data to be created in the warehouse. Names list to be added to the warehouse
    # Inputs: size: the number of cost centers to be breated.
            # start: the starting point for the index. default is 1111. NOTE: if there is data in the warehouse use the last index+1 as the start and switch the write line to the append option


    delta =spark.read.format('delta').table("d_localities")
    locality_structures = delta.toPandas()

    delta =spark.read.format('delta').table("names")
    names = delta.toPandas()

    delta =spark.read.format('delta').table("d_employeeContracts")
    employeeContracts = delta.toPandas()

    size = size
    start = start
    dfIndex = range(start, start + size)

    employees = pd.DataFrame(index=dfIndex)

    # employees['employeeID'] = random.sample(range(1111,9999), size)
    employees = employees.reset_index().rename(columns={'index': 'employeeID'})

    employees['employeeContractID'] = employeeContracts['employeeContractID'].sample(size, replace = False).reset_index(drop=True)

    employees['cityID'] = locality_structures['cityID'].sample(size, replace = True).reset_index(drop=True)

    employees['fullName'] = random.sample(names.Name.to_list(), size)

    employees['gender'] = np.random.choice(['M', 'F'], size=size)

    employees['isManager'] = np.random.choice([0, 1], size=size, p=[0.8, 0.2])

    employees_spark = spark.createDataFrame(employees)

    # employees_spark.write.mode("append").format("delta").saveAsTable("d_employees")
    employees_spark.write.mode("overwrite").format("delta").saveAsTable("d_employees")

    return True

# COMMAND ----------

# TODO add fact table
