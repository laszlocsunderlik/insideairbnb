from airflow import DAG
from airflow.utils.dates import days_ago

from custom.operators import AirbnbFetchDataOperator, AirbnbRankNeighbourhoodsOperator
from custom.sensors import AirbnbApiSensor, JsonDataSensor
from config import settings

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]

with DAG(
    dag_id="API_to_JSON_file_with_custom_hook_operator_and_sensor",
    description="Fetches data from the Airbnb API using the Custom Hook, Custom Operator and Custom Sensor.",
    start_date=days_ago(1),
    schedule=None,
    catchup=False,
    default_args={"provide_context": True},
) as dag:
    fetch_sensor_tasks = []
    fetch_data_tasks = []
    rank_data_tasks = []
    json_sensor_tasks = []
    for export_date in export_dates:
        task_id_count = f"rankin_neighbourhoods_{export_date}"
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
            task_id = f"api_health_check_{endpoint}_{export_date}"
            fetch_sensor = AirbnbApiSensor(
                task_id=task_id,
                conn_id="airbnbapi",
                export_date=export_date,
                poke_interval=5,
                timeout=10,
            )
            fetch_sensor_tasks.append(fetch_sensor)

            task_id = f"fetch_data_{endpoint}_{export_date}"
            fetch_operator = AirbnbFetchDataOperator(
                task_id=task_id,
                conn_id="airbnbapi",
                endpoint=endpoint,
                export_date=export_date,
                out_path=f"{settings.DATA_PATH}/{endpoint}_{export_date}.json",
            )
            fetch_data_tasks.append(fetch_operator)

            json_sensor = JsonDataSensor(
                task_id=f"wait_for_json_data_{endpoint}_{export_date}",
                filepath=f"{settings.DATA_PATH}/{endpoint}_{export_date}.json",
                poke_interval=30,
                timeout=60,
            )
            json_sensor_tasks.append(json_sensor)

    # Set task dependencies
    # Set task dependencies between fetch_data_tasks and rank_data_tasks
    for i in range(0, len(fetch_data_tasks), 2):
        # Connect two fetch_data_tasks to one rank_data_task
        fetch_sensor_tasks[i] >> fetch_data_tasks[i] >> json_sensor_tasks[i] >> rank_data_tasks[i // 2]
        fetch_sensor_tasks[i + 1] >> fetch_data_tasks[i + 1] >> json_sensor_tasks[i +1] >> rank_data_tasks[i // 2]

    # Set task dependencies within fetch_data_tasks
    for i in range(2, len(fetch_data_tasks)):
        fetch_sensor_tasks[i - 2] >> fetch_data_tasks[i - 2] >> json_sensor_tasks[i -2] >> fetch_sensor_tasks[i] >> fetch_data_tasks[i] >> json_sensor_tasks[i]
