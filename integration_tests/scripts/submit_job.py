from databricks_api import DatabricksAPI
from os import getenv
from time import sleep

db = DatabricksAPI(
    host=getenv("DATABRICKS_HOST"), token=getenv("DATABRICKS_AAD_TOKEN")
)

clean_job_details = db.jobs.submit_run(
    run_name="Run tests",
    timeout_seconds=3600,
    tasks=[
        {
            "task_key": "Clean",
            "existing_cluster_id": getenv("DATABRICKS_CLUSTER_ID"),
            "notebook_task": {
                "notebook_path": "/Shared/Testing/cleanup",
            }
        }
    ],
    version="2.1",
)

breakout = 20
iter = 1

task_running = True
while task_running:

    job_details = db.jobs.get_run(clean_job_details["run_id"])

    print(job_details)

    if "result_state" in job_details["state"]:
        task_running = False

    if iter > breakout:
        raise Exception(f"Reached {breakout} attempts and job not resolved")
    iter += 1

    if task_running:
        sleep(60)
