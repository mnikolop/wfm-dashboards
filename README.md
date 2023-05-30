# Work Force Management (WFM) Synthetic Data Generation and Dashboards

## Project premise & purpose

The project endeavour to produce code that generates realistic seaming work force management data for the purpose of reporting and analysis testing.
The project uses the system [Quinyx](https://www.quinyx.com/) as a template for the source data.

## Project components

This project has 3 parts:

1. A data model
   1. Original data names from Quinix
   1. Standard Data​ - 3NF​
   1. Dimensional Model
1. Synthetic data generator
1. Power BI dashboard

## Technologies

The project uses the following tools/technologies:

1. Databricks Notebooks
1. Databricks Delta Tables
1. Databricks warehouse with hive metastore
1. PowerBI

## Implementation

### Data model creation

Initially, the Quinyx API returns were listed and investigated for usefulness and ease of comprehension.
Fields were marked as usable for a planning dashboard and descriptions of their content were given, as well as a general data profiling was performed.

The standard data was created following the 3rd normal form (3NF). This approach was chosen due to 3NFs reduction of duplication of information.
During this step, additional fields and/or entities were created to create a more complete picture and fulfil the needs of the eventual reports and dashboards.

A dimensional model was created by identifying the existing entities as dimensions or facts and any needed fact tables being added. This step allowed for the creation of most of the needed calculations for the planned reports and dashboards.

### Data generator

The data generator was made using python in a databricks notebooks.

Any data files needed as ranges for the data generator were uploaded as csv in the warehouse and loaded in the notebooks with an SQL call.

Pandas datasets were created for each of the entities in the dimentional model, with the exception of fields that were identified as not needed for the reports in the end. Each generated dataset was written back to the databrikcs warehouse as a delta table with the override option (see the [todo](#todo) section).

### Reports & Dashboards

---

## TODO

1. make it so the scripts can be run continually to give the perception of real-time data generation, with append in the delta tables (instead of override).
1. Add human error in schedules.

### Shifts

1. Make sure they cover the schedule (-breaks) perfectly.

### Employees

1. Add fact table containing count of employees

### Employee Schedules

1. d EmployeeSchedule costCenterID - # this needs to be the employee's 80% and a random ne 20%
1. when adding new lines windows will be from today till 2-3 months in the future. If date exists set edited to today()
1. take into account costCenterSchedulePublishedDate for end period of schedule publishing
1. d EmployeeSchedule originalEmployeeScheduleId - if isDeleted = true then copy employeeScheduleID else 0
