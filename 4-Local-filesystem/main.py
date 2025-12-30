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

# MAGIC %run ./locality-structures

# COMMAND ----------

# MAGIC %run ./customers

# COMMAND ----------

# MAGIC %run ./employees

# COMMAND ----------

# MAGIC %run ./employeeScedules

# COMMAND ----------

# MAGIC %run ./shifts
