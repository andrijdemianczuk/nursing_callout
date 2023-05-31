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

13. If you want to schedule all of these notebooks in a pipeline, create a new workflow with the following tasks:
* a. 00_Generate_Callout
* b. 00_Process_Callout
* c. 01_Clean_and_Augment
* d. 02_Aggregate

14. The following configuration can alternatively be used to bootstrap your workflow config:
```json
{
    "job_id": 339532994781288,
    "creator_user_name": "user.name@databricks.com",
    "run_as_user_name": "user.name@databricks.com",
    "run_as_owner": true,
    "settings": {
        "name": "Nurse_Callout_Workflow",
        "email_notifications": {
            "no_alert_for_skipped_runs": false
        },
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "00_Generate_Callout",
                "notebook_task": {
                    "notebook_path": "/Repos/<user.name@databricks.com>m/nursing_callout/python/00_Generate_Callout",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Nurse_Callout_Job_Cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            },
            {
                "task_key": "00_Process_Callout",
                "depends_on": [
                    {
                        "task_key": "00_Generate_Callout"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/<user.name@databricks.com>/nursing_callout/python/00_Process_Callout",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Nurse_Callout_Job_Cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            },
            {
                "task_key": "01_Clean_and_Augment",
                "depends_on": [
                    {
                        "task_key": "00_Process_Callout"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/<user.name@databricks.com>/nursing_callout/python/01_Clean_and_Augment",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Nurse_Callout_Job_Cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            },
            {
                "task_key": "02_Aggregate",
                "depends_on": [
                    {
                        "task_key": "01_Clean_and_Augment"
                    }
                ],
                "notebook_task": {
                    "notebook_path": "/Repos/<user.name@databricks.com>/nursing_callout/python/02_Aggregate",
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Nurse_Callout_Job_Cluster",
                "timeout_seconds": 0,
                "email_notifications": {},
                "notification_settings": {
                    "no_alert_for_skipped_runs": false,
                    "no_alert_for_canceled_runs": false,
                    "alert_on_last_attempt": false
                }
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Nurse_Callout_Job_Cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "13.1.x-scala2.12",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode"
                    },
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-west-2c",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.xlarge",
                    "custom_tags": {
                        "ResourceClass": "SingleNode"
                    },
                    "enable_elastic_disk": true,
                    "data_security_mode": "SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "num_workers": 0
                }
            }
        ],
        "format": "MULTI_TASK"
    },
    "created_time": 1684875414688
}
```
### Deploying the DLT Pipeline
**IMPORTANT!:** The DLT Pipeline requires the lookup tables from the previous section to be in place in order for this to work. Run steps 2&3 from the above section if not already done.

1. Create a new DLT Pipeline in the Delta Live Tables tab of the Databricks Workflow UI.
2. Switch to JSON view
3. Paste the following in, replacing any variables where necessary:
```json
{
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 1,
                "mode": "ENHANCED"
            }
        }
    ],
    "development": true,
    "continuous": false,
    "channel": "PREVIEW",
    "photon": false,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/{user.name@databricks.com}/nursing_callout/python/10_DLT_Staffing_Pipeline"
            }
        }
    ],
    "name": "nurse_callout_pipeline",
    "edition": "ADVANCED",
    "catalog": "main",
    "configuration": {
        "yearly_hours": "400",
        "user": "{user.name@databricks.com}",
        "catalog": "main",
        "schema": "hls"
    },
    "target": "hls"
}
```
4. Be sure to replace the notebook path with the fully-qualified location of the notebook python/10_DLT_Staffing_Pipeline (typically, just replace username)
5. Update the 'configuration' section with the options of your choosing. For the sake of demo, feel free to play around with the yearly_hours option to determine the average number of hours worked per year to adjust the heavy / light workers assessment.