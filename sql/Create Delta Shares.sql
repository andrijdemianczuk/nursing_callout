-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Sharing

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/databricks/tech-talks/master/images/delta_share_overview.png" width="1000">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Unity Catalog as your Entitlement Layer Managed by Databricks
-- MAGIC As a data provider, you can make your Unity Catalog Metastore act as a Delta Sharing Server and share data on Unity Catalog with other organizations.  
-- MAGIC These organizations can then access the data using open source Apache Spark or pandas on any computing platform (including, but not limited to, Databricks). <br> <br>
-- MAGIC <img src="https://i.ibb.co/QJny676/Screen-Shot-2021-11-16-at-10-46-49-AM.png" width="600" height="480" /><br>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Setting up Delta Sharing
-- MAGIC 
-- MAGIC Delta sharing is facilitated through a dedicated service, and is managed in a similar way to NFS management. Shares are applied as contexts for the server to manage that contain relationships with tables. Recipients are then created and granted permissions to the shares and any table objects contained within.

-- COMMAND ----------

-- DBTITLE 1,First, let's look at who has access to our Delta Sharing server
SHOW RECIPIENTS

-- COMMAND ----------

-- DBTITLE 1,Show what shares are set up on our Delta Sharing server
SHOW SHARES

-- COMMAND ----------

DROP RECIPIENT IF EXISTS ademianczuk_vch_internal;
DROP RECIPIENT IF EXISTS ademianczuk_vch_external;
DROP SHARE IF EXISTS ademianczuk_vch_internal;
DROP SHARE IF EXISTS ademianczuk_vch_external;

-- COMMAND ----------

-- DBTITLE 1,Unity Catalog’s security model is based on standard ANSI SQL, to grant permissions at the level of databases, tables, views, rows and columns 
USE CATALOG ademianczuk

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Creating shares

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS ademianczuk_vch_internal COMMENT 'VCH Internal Share. This share will only be for internal workspace sharing';

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS ademianczuk_vch_external COMMENT 'VCH External Share, This share will be for external client sharing';

-- COMMAND ----------

DESCRIBE SHARE ademianczuk_vch_external

-- COMMAND ----------

DESCRIBE SHARE ademianczuk_vch_internal

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Adding tables to shares
-- MAGIC 
-- MAGIC Working with shares is similar to working with schemas and tables. Permissions can be granted at the share or table scope

-- COMMAND ----------

-- DBTITLE 1,Using a share is similar to working schemas. Tables can be quickly added or removed.
ALTER SHARE ademianczuk_vch_internal ADD TABLE ademianczuk.hls.g_unfilled_summary;
ALTER SHARE ademianczuk_vch_external ADD TABLE ademianczuk.hls.g_unfilled_summary;

ALTER SHARE ademianczuk_vch_internal ADD TABLE ademianczuk.hls.s_callout_augmented;
ALTER SHARE ademianczuk_vch_external ADD TABLE ademianczuk.hls.s_callout_augmented;

ALTER SHARE ademianczuk_vch_internal ADD TABLE ademianczuk.hls.s_staff_augmented;
ALTER SHARE ademianczuk_vch_external ADD TABLE ademianczuk.hls.s_staff_augmented;

ALTER SHARE ademianczuk_vch_internal ADD TABLE ademianczuk.hls.s_unfilled_callouts;

-- COMMAND ----------

-- DBTITLE 1,Using the 'show' verb is useful to help determine the attributes belonging to a share
SHOW ALL IN SHARE ademianczuk_vch_internal

-- COMMAND ----------

SHOW ALL IN SHARE ademianczuk_vch_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Creating recipients

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### A note on recipients
-- MAGIC Recipients are individuals who have access to shares and share resources. Each recipient can consume the contents of a share with a personalized access token that is unique. This is really useful for limited time sharing with external parties. Each recipient will receive a one-time credential access download link containing the Delta Sharing endpoint and auth bearer token with a pre-defined expiry period.
-- MAGIC 
-- MAGIC <br/>
-- MAGIC <img src="files/Users/andrij.demianczuk@databricks.com/Activation_Link.png" width=1000 />

-- COMMAND ----------

DROP RECIPIENT IF EXISTS ademianczuk_vch_internal;
CREATE RECIPIENT ademianczuk_vch_internal;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC "shareCredentialsVersion":1 <br/>
-- MAGIC "bearerToken":"2ptbNCy_QK7oQirNWvz6C_JBEcXCnY8PgdqK9vqv7e0rRGm0Q8vwjZPtRp8I9Mh0" <br/>
-- MAGIC "endpoint":"https://nvirginia.cloud.databricks.com/api/2.0/delta-sharing/metastores/2e4e2c23-e42e-439b-ba5f-d637fcb3af46" <br/>
-- MAGIC "expirationTime":"2023-06-26T18:22:19.962Z"

-- COMMAND ----------

DROP RECIPIENT IF EXISTS ademianczuk_vch_external;
CREATE RECIPIENT ademianczuk_vch_external;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC "shareCredentialsVersion":1 <br/>
-- MAGIC "bearerToken":"zPRPqYQ3NIs-nv1oRwEZL8rj0n9-5aUngK-lepMufhFUKfh80xSOGvWA4TdAXyC-" <br/>
-- MAGIC "endpoint":"https://nvirginia.cloud.databricks.com/api/2.0/delta-sharing/metastores/2e4e2c23-e42e-439b-ba5f-d637fcb3af46" <br/>
-- MAGIC "expirationTime":"2023-06-26T18:23:13.582Z"}

-- COMMAND ----------

SHOW RECIPIENTS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Creating recipient permissions on shares

-- COMMAND ----------

-- DBTITLE 1,Recipient permissions are handled the same way as table and share permissions
GRANT SELECT ON SHARE ademianczuk_vch_internal TO RECIPIENT ademianczuk_vch_internal;
GRANT SELECT ON SHARE ademianczuk_vch_external TO RECIPIENT ademianczuk_vch_internal;
GRANT SELECT ON SHARE ademianczuk_vch_external TO RECIPIENT ademianczuk_vch_external;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Auditing permissions
-- MAGIC 
-- MAGIC These are several methods to audit access and permissions on various objects and namespaces. Show and Describe are generally available on most Delta Sharing objects

-- COMMAND ----------

SHOW GRANT ON SHARE ademianczuk_vch_internal;

-- COMMAND ----------

SHOW GRANT ON SHARE ademianczuk_vch_external;

-- COMMAND ----------

DESCRIBE RECIPIENT ademianczuk_vch_internal;

-- COMMAND ----------

DESCRIBE RECIPIENT ademianczuk_vch_external;

-- COMMAND ----------

SHOW GRANT TO RECIPIENT ademianczuk_vch_internal;

-- COMMAND ----------

SHOW GRANT TO RECIPIENT ademianczuk_vch_external;

-- COMMAND ----------

--REVOKE SELECT ON SHARE ademianczuk_vch_external FROM RECIPIENT ademianczuk_vch_external

-- COMMAND ----------

SHOW ALL IN SHARE ademianczuk_vch_internal

-- COMMAND ----------

SHOW ALL IN SHARE ademianczuk_vch_external
