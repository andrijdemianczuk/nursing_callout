-- Databricks notebook source
-- DBTITLE 1,Preview the initial bronze table
SELECT *
FROM ademianczuk.hls.b_staff

-- COMMAND ----------

-- DBTITLE 1,Augment and create the silver table
DROP TABLE IF EXISTS ademianczuk.hls.s_staff_augmented;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.s_staff_augmented
PARTITIONED BY(Unit, Shift)
AS (SELECT *, 
  HrsThisYear / (SELECT avg(HrsThisYear) FROM ademianczuk.hls.b_staff) as HrsThisYearWeight,
  AvgWeeklyHrs / (SELECT avg(AvgWeeklyHrs) FROM ademianczuk.hls.b_staff) as AvgWeeklyWeight
  FROM ademianczuk.hls.b_staff)

-- COMMAND ----------

-- DBTITLE 1,View the silver table
SELECT *
FROM ademianczuk.hls.s_staff_augmented

-- COMMAND ----------

-- DBTITLE 1,Join and create the second silver table
DROP TABLE IF EXISTS ademianczuk.hls.s_callout_augmented;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.s_callout_augmented
PARTITIONED BY(Facility)
AS (SELECT c.`Date`, c.Shift, c.Unit, c.Number_Needed, c.Reason, c.Facility, f.`Long` as `Facility_Name`, c.Filled, m.Description as `unit_description`
  FROM ademianczuk.hls.b_callout c
  JOIN ademianczuk.hls.l_nc_facilities f ON c.Facility = f.Short
  JOIN ademianczuk.hls.l_nc_medunits m on c.Unit = m.Short)

-- COMMAND ----------

-- DBTITLE 1,Preview the second table
SELECT *
FROM ademianczuk.hls.s_callout_augmented

-- COMMAND ----------

-- DBTITLE 1,Create a table with only unfilled callouts
DROP TABLE IF EXISTS ademianczuk.hls.s_unfilled_callouts;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.s_unfilled_callouts
PARTITIONED BY(Shift)
AS (SELECT *
  FROM ademianczuk.hls.s_callout_augmented
  WHERE Filled == FALSE)
