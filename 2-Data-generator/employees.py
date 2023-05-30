# Databricks notebook source
import pandas as pd
import numpy as np
import random
import datetime


# COMMAND ----------

# MAGIC %sql
# MAGIC USE `hive_metastore`.`da_markella_nikolopoulou_themeli_rx1k_delp`;

# COMMAND ----------

delta =spark.read.format('delta').table("d_costCenters")
costCenters = delta.toPandas()


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

size = 500
dfIndex = range(0,size)
columns=['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

data = np.random.choice([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1], size=(size*len(columns)))

data = data.reshape(size, len(columns))

baseSchedule = pd.DataFrame(columns=columns, index=dfIndex, data=data)



# COMMAND ----------

size = 500
start = 1111
dfIndex = range(start, start + size)

employeeContracts = pd.DataFrame(index=dfIndex)

# employeeContracts['employeeContractID'] = random.sample(range(1111,9999), size)
employeeContracts = employeeContracts.reset_index().rename(columns={'index': 'employeeContractID'})

employeeContracts['costCenterID']  = costCenters.costCenterID.sample(size, replace = True).reset_index(drop=True)

employmentStartDateStart = datetime.datetime(year=datetime.datetime.now().year - 10, month=1, day=1)
employmentStartDateEnd = datetime.datetime(year=datetime.datetime.now().year , month=1, day=1)
employeeContracts['contractStartDate'] = np.random.random(size=(size,1)) * (employmentStartDateEnd - employmentStartDateStart) + employmentStartDateStart

employmentEndDateStart = datetime.datetime(year=datetime.datetime.now().year + 1, month=1, day=1)
employmentEndDateEnd = datetime.datetime(year=datetime.datetime.now().year + 5, month=1, day=1)
employeeContracts['contractEndDate'] = np.random.random(size=(size,1)) * (employmentEndDateEnd - employmentEndDateStart) + employmentEndDateStart

employeeContracts['isActive'] = (employeeContracts['contractEndDate'] > (datetime.datetime.today())) & (employeeContracts['contractStartDate'] < datetime.datetime.today()) # because the date windows are set up so they don't need to check if they are properly made this will always be True.

employeeContracts['baseSchedule'] = baseSchedule[columns].to_numpy().tolist()

employeeContracts['totalWeeklyHours'] = np.sum((baseSchedule[columns]*8), axis=1)

employeeContracts['employmentClass'] = np.random.choice([1, 2, 3, 4, 5], size=size)






# COMMAND ----------

employeeContracts

# COMMAND ----------

employeeContracts_spark = spark.createDataFrame(employeeContracts)


# COMMAND ----------

# employeeContracts_spark.write.mode("append").format("delta").saveAsTable("d_employeeContracts")
employeeContracts_spark.write.mode("overwrite").format("delta").saveAsTable("d_employeeContracts")


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

delta =spark.read.format('delta').table("d_localities")
locality_structures = delta.toPandas()

# COMMAND ----------

delta =spark.read.format('delta').table("names")
names = delta.toPandas()

# COMMAND ----------

size = 500
start = 1111
dfIndex = range(start, start + size)

employees = pd.DataFrame(index=dfIndex)

# employees['employeeID'] = random.sample(range(1111,9999), size)
employees = employees.reset_index().rename(columns={'index': 'employeeID'})

employees['employeeContractID'] = employeeContracts['employeeContractID'].sample(size, replace = False).reset_index(drop=True)

employees['cityID'] = locality_structures['cityID'].sample(size, replace = True).reset_index(drop=True)

employees['fullName'] = random.sample(names.Name.to_list(), size)

employees['gender'] = np.random.choice(['M', 'F'], size=size)

employees['isManager'] = np.random.choice([0, 1], size=size, p=[0.8, 0.2])



# COMMAND ----------

employees

# COMMAND ----------

# TODO add fact table

# COMMAND ----------

employees_spark = spark.createDataFrame(employees)


# COMMAND ----------

# employees_spark.write.mode("append").format("delta").saveAsTable("d_employees")
employees_spark.write.mode("overwrite").format("delta").saveAsTable("d_employees")
