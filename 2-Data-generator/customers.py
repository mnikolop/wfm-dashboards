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

# d	CustomerContracts	customerContractID
# d	CustomerContracts	contractEndDate
# d	CustomerContracts	contractStartDate
# d	CustomerContracts	contractHours
# d	CustomerContracts	contractManHoursFactor
# d	CustomerContracts	contractPeriodBaselineTimeWindow -
# d	CustomerContracts	costCenterID
# d	CustomerContracts	productID
# d	CustomerContracts	productTitle
# d	CustomerContracts	workOrderNumber
# d	CustomerContracts	workOrderStartDate


# COMMAND ----------

size = 50
start = 1111
dfIndex = range(start, start + size)

customerContracts = pd.DataFrame(index=dfIndex)

contractEndDateStart = datetime.datetime(year=datetime.datetime.now().year + 1, month=1, day=1)
contractEndDateEnd = datetime.datetime(year=datetime.datetime.now().year + 5, month=1, day=1)
customerContracts['contractEndDate'] = np.random.random(size=(size,1)) * (contractEndDateEnd - contractEndDateStart) + contractEndDateStart

contractStartDateStart = datetime.datetime(year=datetime.datetime.now().year - 5, month=1, day=1)
contractStartDateEnd = datetime.datetime(year=datetime.datetime.now().year, month=1, day=1)
customerContracts['contractStartDate'] = np.random.random(size=(size,1)) * (contractEndDateEnd - contractEndDateStart) + contractEndDateStart

customerContracts['contractHours'] = np.random.randint(low=8, high=40, size=size)

customerContracts['contractManHoursFactor'] = np.random.randint(low=1, high=5, size=size)

customerContracts['costCenterID'] = random.sample(costCenters.costCenterID.to_list(), size)

workOrderStartDateStart = datetime.datetime(year=datetime.datetime.now().year - 5, month=1, day=1)
workOrderStartDateEnd = datetime.datetime(year=datetime.datetime.now().year + 5, month=1, day=1)
customerContracts['workOrderStartDate'] = np.random.random(size=(size,1)) * (workOrderStartDateEnd - workOrderStartDateStart) + workOrderStartDateStart

customerContracts['workOrderNumber'] = random.sample(range(1111,9999), size)

# customerContracts['customerContractID'] = random.sample(range(1111,9999), size)
customerContracts = customerContracts.reset_index().rename(columns={'index': 'customerContractID'})



# COMMAND ----------

customerContracts

# COMMAND ----------

customerContracts_spark = spark.createDataFrame(customerContracts)


# COMMAND ----------

# customerContracts_spark.write.mode("append").format("delta").saveAsTable("d_customerContracts")
customerContracts_spark.write.mode("overwrite").format("delta").saveAsTable("d_customerContracts")


# COMMAND ----------

# d	Customers	customerID 
# d	Customers	customerContractID
# d	Customers	CustomerGroupCategory - 
# d	Customers	CustomerGroupKey - 
# d	Customers	CustomerGroupTitle - 
# d	Customers	customerName


# COMMAND ----------

delta =spark.read.format('delta').table("companies_list")
companies_list = delta.toPandas()

# COMMAND ----------

size = 50
start = 1111
dfIndex = range(start, start + size)

customers = pd.DataFrame(index=dfIndex)


# customers['customerID'] = random.sample(range(1111,9999), size)
customers = customers.reset_index().rename(columns={'index': 'customerID'})

customers['customerName'] = random.sample(companies_list.Name.to_list(), size)

customers['customerContractID'] = customerContracts.customerContractID


# COMMAND ----------

customers

# COMMAND ----------

customers_spark = spark.createDataFrame(customers)


# COMMAND ----------

# customers_spark.write.mode("append").format("delta").saveAsTable("d_customers")
customers_spark.write.mode("overwrite").format("delta").saveAsTable("d_customers")
