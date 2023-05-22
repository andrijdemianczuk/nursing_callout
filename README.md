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
  a. USER: Set to your username
  b. Catalog: Set to the catalog of your choice (main isn't a bad option if you don't know). This Catalog MUST exist and must be unity-enabled.

## SQL Quickstart
