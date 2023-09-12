# Databricks notebook source
import pandas as pd
import numpy as np
import random
import datetime


# COMMAND ----------

# MAGIC %sql
# MAGIC USE `hive_metastore`.`da_markella_nikolopoulou_themeli_rx1k_delp`;

# COMMAND ----------

localities = pd.read_csv('4-Local-filesystem/ranges/localities.csv')
localities.columns = ['countyID', 'countyName', 'municipalityID', 'municipalityName', 'cityID', 'cityName']


# COMMAND ----------

# d	BusinessUnits	businessUnitCode
# d	BusinessUnits	businessUnitName
# d	Cities	cityName = ort
# d	Cities	cityID
# d	Counties	countyID
# d	Counties	countyName
# d	Districts	districtID
# d	Districts	districtName = region
# d	MarketAreas	marketAreaID
# d	MarketAreas	marketAreaName
# d	Municipalities	municipalityID = kommun
# d	Municipalities	municipalityName



# COMMAND ----------

localities['marketAreaID'] = localities.countyID
localities['marketAreaName'] = localities.countyName
localities['businessUnitCode'] = localities.municipalityID
localities['businessUnitName'] = localities.municipalityName

# COMMAND ----------

# localities

# COMMAND ----------



# COMMAND ----------

localities.to_csv('4-Local-filesystem/data-out/d_localities.csv')  


# COMMAND ----------

# d	CostCenters	activeFrom
# d	CostCenters	activeTo
# d	CostCenters	cityID
# d	CostCenters	costCenterID
# d	CostCenters	costCenterSchedulePublishedDate
# d	CostCenters	costCenterName
# d	CostCenters	costCenterTypeID - 
# d	CostCenters	countyID
# d	CostCenters	districtID - 
# d	CostCenters	edited
# d	CostCenters	hasPersonnel
# d	CostCenters	marketAreaID
# d	CostCenters	municipalityID
 


# COMMAND ----------

size = 500
start = 1111
dfIndex = range(start, start + size)

costCenters = pd.DataFrame(index=dfIndex)


activeFromStart = datetime.datetime(1990, 1, 1)
activeFromEnd = datetime.datetime(year=datetime.datetime.now().year, month=1, day=1)
costCenters['activeFrom'] = np.random.random(size=(size,1)) * (activeFromEnd - activeFromStart) + activeFromStart

activeToStart = datetime.datetime(year=datetime.datetime.now().year+1, month=1, day=1)
activeToEnd = datetime.datetime(year=datetime.datetime.now().year+50, month=1, day=1)
costCenters['activeTo'] = np.random.random(size=(size,1)) * (activeToEnd - activeToStart) + activeToStart

costCenters['costCenterSchedulePublishedDate'] = random.random() * (costCenters.activeTo - datetime.datetime.now()) + datetime.datetime.now()

costCenters['edited'] = random.random() * (datetime.datetime.now() - costCenters.activeFrom) + costCenters.activeFrom

costCenters['hasPersonnel'] = np.random.randint(0,2, size=(size,1))

costCenters['marketAreaID'] = localities.marketAreaID.sample(len(costCenters), replace = True).reset_index(drop=True)

costCenters['municipalityID'] = localities.municipalityID.sample(len(costCenters), replace = True).reset_index(drop=True)

# costCenters['costCenterID'] = random.sample(range(1111,9999), size)
costCenters = costCenters.reset_index().rename(columns={'index': 'costCenterID'})

sampleIDsNames = localities[['cityID', 'cityName']].sample(size, replace = True).reset_index(drop=True)
costCenters['cityID'] = sampleIDsNames.cityID
costCenters['costCenterName'] = sampleIDsNames.cityName

costCenters['countyID'] = localities.countyID.sample(len(costCenters), replace = True).reset_index(drop=True)


# COMMAND ----------

# costCenters

# COMMAND ----------


# COMMAND ----------

costCenters.to_csv('4-Local-filesystem/data-out/d_costCenters.csv')  

