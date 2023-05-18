-- Databricks notebook source
-- DBTITLE 1,Read from CSV and create views
DROP VIEW IF EXISTS NC_Facilities;
DROP VIEW IF EXISTS NC_Positions;
DROP VIEW IF EXISTS NC_MedUnits;

CREATE TEMPORARY VIEW NC_Facilities
USING CSV
OPTIONS (path "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Facilities.csv", header "true", mode "FAILFAST");

CREATE TEMPORARY VIEW NC_Positions
USING CSV
OPTIONS (path "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Nurse_Positions.csv", header "true", mode "FAILFAST");

CREATE TEMPORARY VIEW NC_MedUnits
USING CSV
OPTIONS (path "dbfs:/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Med_Units.csv", header "true", mode "FAILFAST");

-- COMMAND ----------

-- DBTITLE 1,Write views to delta
DROP TABLE IF EXISTS ademianczuk.hls.l_nc_facilities;
CREATE TABLE IF NOT EXISTS ademianczuk.hls.l_nc_facilities
AS SELECT * FROM NC_Facilities;

DROP TABLE IF EXISTS ademianczuk.hls.l_nc_positions;
CREATE TABLE IF NOT EXISTS ademianczuk.hls.l_nc_positions
AS SELECT * FROM NC_Positions;

DROP TABLE IF EXISTS ademianczuk.hls.l_nc_medunits;
CREATE TABLE IF NOT EXISTS ademianczuk.hls.l_nc_medunits
AS SELECT * FROM NC_MedUnits;
