-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Health & Life Sciences: Staffing Data Insights
-- MAGIC <br/>
-- MAGIC <img src="https://imageio.forbes.com/specials-images/imageserve/5dbb4182d85e3000078fddae/0x0.jpg?format=jpg" width="750" />
-- MAGIC 
-- MAGIC ## Introduction
-- MAGIC With modernized facilities, growing staff counts and increasing demand for just-in-time healthcare, staff resourcing is a critical asset for all health care providers. In an effort to maximize staff distribution we will examine how Datbricks and the Lakehouse can help organizations drive better staffing outcomes with data-driven decision making.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Delta Live Tables
-- MAGIC 
-- MAGIC <!-- img src="https://www.databricks.com/en-website-assets/static/3b87b2fcad196ec1127fc9daece8ab26/b4260/DLT_graphic_tiers.webp" width=1000/ -->
-- MAGIC <img src="https://www.databricks.com/en-website-assets/static/280e83341c59377fa2a1c4caa26028c9/c6e46/DLT_graphic_pipeline.webp" width=1000>
-- MAGIC 
-- MAGIC ### Declarative ETL & Live Tables
-- MAGIC 
-- MAGIC Declarative ETL involves the user describing the desired results of the pipeline without explicitly listing the ordered steps that must be performed to arrive at the result. Declarative means focusing on the what "what" is our desired goal and leveraging an intelligent engine like DLT to figure out "how" the compute framework should carry out these processes. This is what Delta Live Tables focus on - outcomes and an easy way to define them to support robust workloads.
-- MAGIC 
-- MAGIC The DLT engine is the GPS that can interpret the map and determine optimal routes and provide you with metrics such as ETA. Details about the neighborhoods that were traversed in the route are like data lineage, and the ability to find detours around accidents (or bugs) is a result of dependency resolution and modularity which is afforded by the declarative nature of DLT.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Bronze Tables

-- COMMAND ----------

-- DBTITLE 1,Create the lookup tables
CREATE OR REFRESH STREAMING LIVE TABLE dlt_hls_facilities
COMMENT "The lookup files containing facility information"
TBLPROPERTIES ("quality" = "bronze")
AS (SELECT * FROM cloud_files('/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Facilities/', 'csv', map("header", "true")));

CREATE OR REFRESH STREAMING LIVE TABLE dlt_hls_positions
COMMENT "The lookup files containing position information"
TBLPROPERTIES ("quality" = "bronze")
AS (SELECT * FROM cloud_files('/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Nurse_Positions/', 'csv', map("header", "true")));

CREATE OR REFRESH STREAMING LIVE TABLE dlt_hls_medunits
COMMENT "The lookup files containing medical unit information"
TBLPROPERTIES ("quality" = "bronze")
AS (SELECT * FROM cloud_files('/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Med_Units/', 'csv', map("header", "true")));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://www.logolynx.com/images/logolynx/2a/2ad00c896e94f1f42c33c5a71090ad5e.png" width=100>
-- MAGIC 
-- MAGIC ## The Challenge
-- MAGIC 
-- MAGIC Understanding staff callout structure is a complex process. Matching available, qualified staff with shift openings is a challenge due to the constantly changing needs of facilities. Staff have different circumstances and needs making the process fairly manual. The goals for the staffing report are summarized as:
-- MAGIC 
-- MAGIC * Analyze recent staffing requirements
-- MAGIC * Identify relationships for unfulfilled requests
-- MAGIC * Understand the distribution of callouts and facilities
-- MAGIC * Provide up-to-date reporting on openings and availability

-- COMMAND ----------

-- DBTITLE 1,Load the data from CSV
CREATE OR REFRESH STREAMING LIVE TABLE dlt_hls_callouts
COMMENT "The table containing all of the callout records"
TBLPROPERTIES ("quality" = "bronze")
AS (SELECT * FROM cloud_files('/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Callouts/', 'csv', map("header", "true")));

CREATE OR REFRESH STREAMING LIVE TABLE dlt_hls_staffing
COMMENT "The table with all of the staffing data"
TBLPROPERTIES ("quality" = "bronze")
AS (SELECT * FROM cloud_files('/FileStore/Users/andrij.demianczuk@databricks.com/data/hls_source/NC_Staffing/', 'csv', map("header", "true")));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Data Evolution Explained
-- MAGIC 
-- MAGIC <img src="https://www.databricks.com/en-website-assets/static/3b87b2fcad196ec1127fc9daece8ab26/b4260/DLT_graphic_tiers.webp" width=750>
-- MAGIC 
-- MAGIC - The **Bronze** layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc. The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage), data lineage, auditability, reprocessing if needed without rereading the data from the source system.
-- MAGIC - In the **Silver** layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. (e.g. master customers, stores, non-duplicated transactions and cross-reference tables).The Silver layer brings the data from different sources into an Enterprise view and enables self-service analytics for ad-hoc reporting, advanced analytics and ML. It serves as a source for Departmental Analysts, Data Engineers and Data Scientists to further create projects and analysis to answer business problems via enterprise and departmental data projects in the Gold Layer.In the lakehouse data engineering paradigm, typically the ELT methodology is followed vs. ETL - which means only minimal or "just-enough" transformations and data cleansing rules are applied while loading the Silver layer. Speed and agility to ingest and deliver the data in the data lake is prioritized, and a lot of project-specific complex transformations and business rules are applied while loading the data from the Silver to Gold layer. From a data modeling perspective, the Silver Layer has more 3rd-Normal Form like data models. Data Vault-like, write-performant data models & can be used in this layer.
-- MAGIC - Data in the **Gold** layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules are applied here. Final presentation layer of projects such as Customer Analytics, Product Quality Analytics, Inventory Analytics, Customer Segmentation, Product Recommendations, Marking/Sales Analytics etc. fit in this layer. We see a lot of Kimball style star schema-based data models or Inmon style Data marts fit in this Gold Layer of the lakehouse. So you can see that the data is curated as it moves through the different layers of a lakehouse. In some cases, we also see that lot of Data Marts and EDWs from the traditional RDBMS technology stack are ingested into the lakehouse, so that for the first time Enterprises can do "pan-EDW" advanced analytics and ML - which was just not possible or too cost prohibitive to do on a traditional stack. (e.g. IoT/Manufacturing data is tied with Sales and Marketing data for defect analysis or health care genomics, EMR/HL7 clinical data markets are tied with financial claims data to create a Healthcare Data Lake for timely and improved patient care analytics.)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="https://miro.medium.com/v2/resize:fit:816/1*nTunKZq-WnfjUpVD6-kRJw.png" width=400>
-- MAGIC 
-- MAGIC Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.
-- MAGIC 
-- MAGIC Instead of defining your data pipelines using a series of separate Apache Spark tasks, you define streaming tables and materialized views that the system should create and keep up to date. Delta Live Tables manages how your data is transformed based on queries you define for each processing step. You can also enforce data quality with Delta Live Tables expectations, which allow you to define expected data quality and specify how to handle records that fail those expectations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Silver Tables

-- COMMAND ----------

-- DBTITLE 1,Create a new version of the staffing table with weight based on hours
CREATE STREAMING LIVE TABLE dlt_hls_staff_augmented(
  CONSTRAINT valid_order_number EXPECT (HrsThisYear >= 260) ON VIOLATION DROP ROW
)
PARTITIONED BY (Unit, Shift)
COMMENT "The augmented staff data enriched with weights"
TBLPROPERTIES ("quality" = "silver")
AS (SELECT *, 
  HrsThisYear / (SELECT avg(HrsThisYear) FROM LIVE.dlt_hls_staffing) as HrsThisYearWeight,
  AvgWeeklyHrs / (SELECT avg(AvgWeeklyHrs) FROM LIVE.dlt_hls_staffing) as AvgWeeklyWeight
  FROM STREAM(LIVE.dlt_hls_staffing))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contstraints & Expectations
-- MAGIC You use expectations to define data quality constraints on the contents of a dataset. Expectations allow you to guarantee data arriving in tables meets data quality requirements and provide insights into data quality for each pipeline update. You apply expectations to queries using Python decorators or SQL constraint clauses.
-- MAGIC 
-- MAGIC Expectations are optional clauses you add to Delta Live Tables dataset declarations that apply data quality checks on each record passing through a query.
-- MAGIC An expectation consists of three things:
-- MAGIC * A description, which acts as a unique identifier and allows you to track metrics for the constraint.
-- MAGIC * A boolean statement that always returns true or false based on some stated condition.
-- MAGIC * An action to take when a record fails the expectation, meaning the boolean returns false.
-- MAGIC 
-- MAGIC In the example above, we imposed a simple expectation to report only on a minimum number of hours as a requirement for a nurse to practice with a valid license
-- MAGIC ```SQL
-- MAGIC CONSTRAINT valid_order_number EXPECT (HrsThisYear >= 260) ON VIOLATION DROP ROW
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Create a joined view of the callout data
CREATE STREAMING LIVE TABLE dlt_hls_callout_augmented
PARTITIONED BY (Facility)
COMMENT "The augmented callout data, enriched with facility and callout data"
TBLPROPERTIES ("quality" = "silver")
AS (
  SELECT c.`Date`, c.Shift, c.Unit, c.Number_Needed, c.Reason, c.Facility, f.`Long` as `Facility_Name`, c.Filled, m.Description as `unit_description`
  FROM STREAM(LIVE.dlt_hls_callouts) c
  JOIN LIVE.dlt_hls_facilities f ON c.Facility = f.Short
  JOIN LIVE.dlt_hls_medunits m on c.Unit = m.Short)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Performance Tuning
-- MAGIC When considering transformations, it's imporant to always consider where and when we're transforming data. As a general rule, it's best to perform narrow transformations on datasets earlier in the pipeline and wide transformations on datasets that have been reduced as much as possible. This is an important distinction to make since wide transformations are computationally expensive and require that records be compared across Spark RDD partitions.
-- MAGIC 
-- MAGIC ### A quick note on partitions
-- MAGIC It's also important to acknowledge the difference between Spark RDD partitions and Delta partitions. Spark RDD partitions are utilitarian during in-process events. When a dataset is sent to the worker nodes in a Spark cluster, they are further separated amongst the available blocks to store the data within each worker node. By default Spark is configured to use 200 parititons spread throughout the cluster to split the data up. Although this has a significant performance impact if tuned correctly it this topic is out of scope for this project.
-- MAGIC 
-- MAGIC Delta table partitions on the other hand are how delta tables are broken up and stored in directories as delta files. Since Delta supports file skipping, the biggest performance gains can be had by paritioning tables according to columns that are frequently filtered on. This will imply that only files containing relevant data are evaluated.

-- COMMAND ----------

-- DBTITLE 1,Isolate just the unfilled callouts
CREATE STREAMING LIVE TABLE dlt_hls_unfilled_callouts
PARTITIONED BY (Shift)
COMMENT "Isolate the unfilled callouts for reporting. This also could be done with a quarantine"
TBLPROPERTIES ("quality" = "silver")
AS (SELECT *
  FROM STREAM(live.dlt_hls_callout_augmented)
  WHERE Filled == FALSE)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Gold Tables

-- COMMAND ----------

-- DBTITLE 1,Unfilled callout summary
CREATE STREAMING LIVE TABLE dlt_hls_unfilled_summary

COMMENT "A summary table of the unfilled callouts"
TBLPROPERTIES ("quality" = "gold")
AS (SELECT dayofweek(`Date`) as DOW, `Date`, Shift, Unit, Number_Needed, Reason, Facility
  FROM STREAM(LIVE.dlt_hls_unfilled_callouts))

-- COMMAND ----------

-- DBTITLE 1,Staff availability summary
CREATE STREAMING LIVE TABLE dlt_hls_staff_availability_summary

COMMENT "A summary table of available staff"
TBLPROPERTIES ("quality" = "gold")
AS (SELECT Credentials, Shift, Unit, count(*) AS `count`
  FROM STREAM(live.dlt_hls_staff_augmented)
  GROUP BY Credentials, Shift, Unit)

-- COMMAND ----------

-- DBTITLE 1,Summary of staff absences
CREATE STREAMING LIVE TABLE dlt_hls_staff_absences

COMMENT "A summary table of staff absences"
TBLPROPERTIES ("quality" = "gold")
AS (SELECT `Date`, Shift, Unit, Facility, sum(Number_Needed) as `Total_Needed`
  FROM STREAM(LIVE.dlt_hls_callout_augmented)
  GROUP BY `Date`, Shift, Unit, Facility)

-- COMMAND ----------

-- DBTITLE 1,Identify heavy shift workers
CREATE STREAMING LIVE TABLE dlt_hls_staff_heavy_workers

COMMENT "A summary of heavy scheduled staff"
TBLPROPERTIES ("quality" = "gold")
AS (SELECT *
  FROM STREAM(live.dlt_hls_staff_augmented)
  WHERE HrsThisYearWeight > 1 AND AvgWeeklyWeight > 1)

-- COMMAND ----------

-- DBTITLE 1,Identify light shift workers
CREATE STREAMING LIVE TABLE dlt_hls_staff_light_workers

COMMENT "A summary of light scheduled staff"
TBLPROPERTIES ("quality" = "gold")
AS (SELECT *
  FROM STREAM(live.dlt_hls_staff_augmented)
  WHERE HrsThisYearWeight < 1 AND AvgWeeklyWeight < 1)
