import airflow
from airflow import DAG

from custom.operators import AirbnbFetchDataOperator, AirbnbRankNeighbourhoodsOperator
from config import settings

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]

with DAG(
    dag_id="API_to_JSON_file_with_custom_hook_and_operator",
    description="Fetches data from the Airbnb API using the Custom Hook and Custom Operator.",
    start_date=airflow.utils.dates.days_ago(1),
    schedule=None,
    catchup=False,
    default_args={"provide_context": True},
) as dag:
    fetch_data_tasks = []
    rank_data_tasks = []
    for export_date in export_dates:
        task_id_count = f"count_listings_{export_date}_using_custom_operator"
        count_operator = AirbnbRankNeighbourhoodsOperator(
            task_id=task_id_count,
            input_path_listings=f"{settings.DATA_PATH}/listings_{export_date}.json",
            input_path_neighbourhoods=f"{settings.DATA_PATH}/neighbourhoods_{export_date}.json",
            export_date=export_date,
            output_path=f"{settings.DATA_PATH}/rankings.csv",
            geopandas_kwargs={"driver": "GeoJSON", "crs": "EPSG:4326", "encoding": "utf-8", "index": True},
        )
        rank_data_tasks.append(count_operator)
        for endpoint in ["neighbourhoods", "listings"]:
            task_id = f"export_{endpoint}_{export_date}_using_custom_hook"
            fetch_operator = AirbnbFetchDataOperator(
                task_id=task_id,
                conn_id="airbnbapi",
                endpoint=endpoint,
                export_date=export_date,
                out_path=f"{settings.DATA_PATH}/{endpoint}_{export_date}.json",
            )
            fetch_data_tasks.append(fetch_operator)

    # Set task dependencies
    # Set task dependencies between fetch_data_tasks and rank_data_tasks
    for i in range(0, len(fetch_data_tasks), 2):
        # Connect two fetch_data_tasks to one rank_data_task
        fetch_data_tasks[i] >> rank_data_tasks[i // 2]
        fetch_data_tasks[i + 1] >> rank_data_tasks[i // 2]

    # Set task dependencies within fetch_data_tasks
    for i in range(2, len(fetch_data_tasks)):
        fetch_data_tasks[i - 2] >> fetch_data_tasks[i]
