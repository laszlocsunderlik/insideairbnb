import logging
import json
import os

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from geojson import FeatureCollection

from custom.hooks import AirbnbApiHook

os.environ["no_proxy"] = "*"

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]

with DAG(
        dag_id="API_to_JSON_file_with_custom_hook",
        description="Fetches data from the Airbnb API using the Python Operator and Custom Hook.",
        start_date=airflow.utils.dates.days_ago(1),
        schedule=None,
        catchup=False
) as dag:
    def fetch_data(conn_id, templates_dict, batch_size=100, **_):
        logger = logging.getLogger(__name__)

        endpoint = templates_dict["endpoint"]
        logger.info(f"Fetching data from Airbnb API with endpoint: {endpoint}")

        start_date = templates_dict["start_date"]
        out_path = templates_dict["out_path"]
        logger.info(f"Fetching data from {start_date}")

        hook = AirbnbApiHook(conn_id=conn_id)
        data_pages = hook.get_data(
                endpoint=endpoint, start_date=start_date, batch_size=batch_size
            )

        # Flatten the list of features from all pages
        features = [feature for page in data_pages for feature in page.get("features", [])]

        # Create a GeoJSON FeatureCollection
        feature_collection = FeatureCollection(features)

        logger.info(f"Fetched {len(features)} ratings")
        logger.info(f"Writing ratings to {out_path}")

        output_dir = os.path.dirname(out_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(out_path, "w") as file_:
            json.dump(feature_collection, fp=file_)
        logger.info(f"Data written to {out_path}")
        return None


    tasks = []
    for export_date in export_dates:
        for endpoint in ["neighbourhoods", "listings"]:
            task_id = f"export_{endpoint}_{export_date}_using_custom_hook"
            fetch_operator = PythonOperator(
                task_id=task_id,
                python_callable=fetch_data,
                op_kwargs={"conn_id": "airbnbapi"},
                templates_dict={
                    "endpoint": endpoint,
                    "start_date": export_date,
                    "out_path": f"/Users/csunderliklaszlo/Dev/sandbox/building-api/data/{endpoint}_{export_date}.json"
                },
            )
            tasks.append(fetch_operator)

    # Set task dependencies
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
