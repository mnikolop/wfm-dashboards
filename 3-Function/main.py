# Databricks notebook source
# Step 1: add the needed range files to the metastore 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Mounting the hive metastore
# MAGIC USE `hive_metastore`.`da_markella_nikolopoulou_themeli_rx1k_delp`;

# COMMAND ----------

# Step 3: link to function notebooks and run functions with desired inputs.
# NOTE: here all funcitons that require inputs are run with the default ones.

# COMMAND ----------

# MAGIC %run ./location-structures

# COMMAND ----------

print(localities_def())

# COMMAND ----------

print(costCenters_def(size = 500, start = 1111))

# COMMAND ----------

# MAGIC %run ./customers

# COMMAND ----------

print(customerContracts_def(size = 50, start = 1111))

# COMMAND ----------

print(customers_def(size = 50, start = 1111))

# COMMAND ----------

# MAGIC %run ./employees

# COMMAND ----------

print(employeeContracts_def(sampleSize = 500, size = 500, start = 1111, employmentEndDateStartYears = 1))

# COMMAND ----------

print(employees_def(size = 500, start = 1111))

# COMMAND ----------

# MAGIC %run ./employeeScedules

# COMMAND ----------

print(employeeScedules_def(sampleSize = 50, start = 1111))

# COMMAND ----------

print(f_employeeSchedule_def())

# COMMAND ----------

# MAGIC %run ./shifts

# COMMAND ----------

print(shifts_def(start = 1111))

# COMMAND ----------

print(f_shifts_def())
