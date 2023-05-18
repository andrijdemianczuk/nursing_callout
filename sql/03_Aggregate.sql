-- Databricks notebook source
SELECT *
FROM ademianczuk.hls.s_staff_augmented

-- COMMAND ----------

SELECT *
FROM ademianczuk.hls.s_callout_augmented

-- COMMAND ----------

SELECT dayofweek(`Date`) as DOW, `Date`, Shift, Unit, Number_Needed, Reason, Facility
FROM ademianczuk.hls.s_unfilled_callouts

-- COMMAND ----------

DROP TABLE IF EXISTS ademianczuk.hls.g_unfilled_summary;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.g_unfilled_summary
AS (SELECT dayofweek(`Date`) as DOW, `Date`, Shift, Unit, Number_Needed, Reason, Facility
  FROM ademianczuk.hls.s_unfilled_callouts)

-- COMMAND ----------

DROP TABLE IF EXISTS ademianczuk.hls.g_staff_availability;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.g_staff_availability
AS (SELECT Credentials, Shift, Unit, count(*) AS `count`
  FROM ademianczuk.hls.s_staff_augmented
  GROUP BY Credentials, Shift, Unit)

-- COMMAND ----------

DROP TABLE IF EXISTS ademianczuk.hls.g_staff_absences;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.g_staff_absences
AS (SELECT `Date`, Shift, Unit, Facility, sum(Number_Needed) as `Total_Needed`
  FROM ademianczuk.hls.s_callout_augmented
  GROUP BY `Date`, Shift, Unit, Facility)

-- COMMAND ----------

DROP TABLE IF EXISTS ademianczuk.hls.g_heavy_workers;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.g_heavy_workers
AS (SELECT *
  FROM ademianczuk.hls.s_staff_augmented
  WHERE HrsThisYearWeight > 1 AND AvgWeeklyWeight > 1)

-- COMMAND ----------

DROP TABLE IF EXISTS ademianczuk.hls.g_light_workers;

CREATE TABLE IF NOT EXISTS ademianczuk.hls.g_light_workers
AS (SELECT *
  FROM ademianczuk.hls.s_staff_augmented
  WHERE HrsThisYearWeight < 1 AND AvgWeeklyWeight < 1)
