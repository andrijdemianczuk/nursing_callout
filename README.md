# Nursing Callout Quickstart
This repository contains two versions of project files:
1. A version of the project written in Python
2. A version of the project written (mostly) in SQL

This project is intended to be run from within a Databricks Spark Context. Some code features (e.g., dbutils) are not supported via Spark Connect so unless you are running this from within a managed Databricks environment, your mileage may vary.

This manual cover how to get started both with the Python and SQL versions of the project. Please pick one or the other to get started.

If you need help getting this repo into your managed Databricks environment, please refer to [these docs](https://docs.databricks.com/repos/index.html):

**A Unity Catalog enabled workspace is required**

## Python Quickstart
Deploying the python-based workflows is a fairly straightforward process. You can deploy either or both the workflow and DLT pipeline. The former takes a more traditional ETL stance, leveraging data being committed to bespoke delta tables. The DLT pipeline however works in terms of materializations which are then conceptualized as delta artifacts that can be directly interfaced with.

### Deploying the Python Workflow
1. Clone this repository into your Databricks Workspace if not already done.
2. Open lookups/Init_Lookups and edit the two global variables in cell 1 to match your credentials and schema config:
  ```
  USER: Set to your username
  CATALOG: Set to the catalog of your choice (main isn't a bad option if you don't know). This Catalog MUST exist and must be unity-enabled.
  ```
3. Run the Init_Lookups notebook with the 'Run All' option. This will create a schema in the catalog of your choice called 'hls' which will be used for the remainder of the project. Please remember, that choosing a Unity-enabled catalog is required.
4. Open python/00_Generate_Static_files. This notebook bootstraps the staffing sheet and writes it to a storage location within DBFS. Replace the catalog and user names with the ones from the previous step. e.g., This notebook creates a random list of 1000 staff / employees.
```
USER = "<user.name>@databricks.com"
CATALOG = "main"
```
5. Run the notebook. This should create a new table called 'b_hls_staff'. This will be our bronze staffing table. The intention of this later is to be able to do upserts to this table to showcase CDF / CDC (coming soon)
6. Open python/00_Generate_Callout. This notebook is used to generate a callout series and lands a file as csv in a landing area which will be picked up by autoloader. Like before, update the following at the top of the notebook:
```
USER = "<user.name>@databricks.com"
CATALOG = "main"
```
7. Run the notebook. This notebook is intended to be run as many times as required to generate callouts.
8. **Optional:** If you want to test out the Spark Streaming capabilites, run the python/00_Process_Callout notebook. This will initialize a new stream and display a streaming dataframe. If you want to see new data coming in, leave this stream running and run the pyton/00_Generate_Callout notebook again to see new callout records being appended in real-time.
9. Open python/01_Clean_And_Augment. This notebook converts the bronze tables into silver with some cleaning and augmentation. Update the following at the top of the notebook.
```
CATALOG = "main"
```
10. Run the notebook.
11. Open python/02_Aggregate. This notebook does all of the work to create the summary and aggregate tables. This will create the final 5 gold tables which can be used for reporting and dashboarding. As above, replace the following with the catalog you are using:
```
CATALOG = "main"
```
12. Run the notebook.
### Deploying the DLT Pipeline.
**IMPORTANT!:** The DLT Pipeline requires the lookup tables from the previous section to be in place in order for this to work. Run steps 2&3 from the above section if not already done.