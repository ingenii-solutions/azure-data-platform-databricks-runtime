from databricks_api import DatabricksAPI
from os import getenv
from time import sleep

db = DatabricksAPI(
    host=getenv("DATABRICKS_HOST"), token=getenv("DATABRICKS_AAD_TOKEN")
)

cluster_id = getenv("DATABRICKS_CLUSTER_ID")

clean_job_details = db.jobs.submit_run(
    run_name="Run tests",
    timeout_seconds=3600,
    tasks=[
            {
                "task_key": "Clean",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/cleanup",
                }
            },
            {
                "task_key": "ExtraColumns",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "Clean"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/file_has_extra_columns",
                }
            },
            {
                "task_key": "StatusUpdates",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "ExtraColumns"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/status_updates",
                }
            },
            {
                "task_key": "HappyPath",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "StatusUpdates"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_happy_path",
                }
            },
            {
                "task_key": "MergeInsert",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "HappyPath"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_merge_insert",
                }
            },
            {
                "task_key": "MergeUpdate",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "MergeInsert"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_merge_update",
                }
            },
            {
                "task_key": "Insert",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "MergeUpdate"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_insert",
                }
            },
            {
                "task_key": "Replace",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "Insert"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_replace",
                }
            },
            {
                "task_key": "IngestTestFailures",
                "existing_cluster_id": cluster_id,
                "depends_on": [{"task_key": "Replace"}],
                "notebook_task": {
                    "notebook_path": "/Shared/Testing/ingest_data_test_failures",
                }
            }
    ],
    version="2.1",
)

job_details = db.jobs.get_run(clean_job_details["run_id"])

print("\n\t".join([
    "Job submitted",
    f"Job ID: {job_details['job_id']}, Run ID: {job_details['job_id']}",
    f"URL: {job_details['run_page_url']}"
]))

breakout = 20
iter = 1

task_running = True
while task_running:

    job_details = db.jobs.get_run(clean_job_details["run_id"])

    print(f"Current state: {job_details['state']}")
    for task in job_details["tasks"]:
        if "depends_on" not in task:
            print(f"\tTask: {task['task_key']}, state: {task['state'].get('result_state') or task['state']['life_cycle_state']}")
    for task in job_details["tasks"]:
        if "depends_on" in task:
            print(", ".join([
                f"\tTask: {task['task_key']}",
                f"depends on: {str([dep_task['task_key'] for dep_task in task['depends_on']])}",
                f" state: {task['state'].get('result_state') or task['state']['life_cycle_state']}"
            ]))

    if "result_state" in job_details["state"]:
        task_running = False

    if iter > breakout:
        print(job_details)
        raise Exception(f"Reached {breakout} attempts and job not resolved")
    iter += 1

    if task_running:
        sleep(60)

if job_details["state"]["result_state"] != "SUCCESS":
    print("Result")

    print({k: v for k, v in job_details.items() if k != "tasks"})

    for task in job_details["tasks"]:
        print(", ".join([
            f"\tTask: {task['task_key']}",
            f"depends on: {str([dep_task['task_key'] for dep_task in task.get('depends_on', [])])}",
            f"state: {task['state'].get('result_state') or task['state']}",
            f"run_url: {task['run_page_url']}",
        ]))

    raise Exception("Testing did not complete successfully!")
