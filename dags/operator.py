import airflow
from airflow import DAG

from custom.operators import AirbnbFetchDataOperator

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]

with DAG(
    dag_id="API_to_JSON_file_with_custom_hook_and_operator",
    description="Fetches data from the Airbnb API using the Custom Hook and Custom Operator.",
    start_date=airflow.utils.dates.days_ago(1),
    schedule=None,
    catchup=False
) as dag:
    tasks = []
    for export_date in export_dates:
        for endpoint in ["neighbourhoods", "listings"]:
            task_id = f"export_{endpoint}_{export_date}_using_custom_hook"
            fetch_operator = AirbnbFetchDataOperator(
                task_id=task_id,
                conn_id="airbnbapi",
                endpoint=endpoint,
                start_date=export_date,
                out_path=f"/Users/csunderliklaszlo/Dev/sandbox/building-api/data/{endpoint}_{export_date}.json",
            )
            tasks.append(fetch_operator)

    # Set task dependencies
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]