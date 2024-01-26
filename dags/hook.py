import logging
import json
import os

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from geojson import FeatureCollection
import geopandas as gpd
import pandas as pd

from custom.hooks import AirbnbApiHook
from config import settings

os.environ["no_proxy"] = "*"
logger = logging.getLogger(__name__)

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]

with DAG(
        dag_id="API_to_JSON_file_with_custom_hook",
        description="Fetches data from the Airbnb API using the Python Operator and Custom Hook.",
        start_date=airflow.utils.dates.days_ago(1),
        schedule=None,
        catchup=False,
        default_args={'provide_context': True}
) as dag:
    def _fetch_data(conn_id, templates_dict, batch_size=100, **_):

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
            json.dump(feature_collection, fp=file_, indent=4)
        logger.info(f"Data written to {out_path}")
        return None


    def _rank_neighbourhoods(templates_dict: dict, **_):
        input_path_listings = templates_dict["input_path_listings"]
        input_path_neighbourhoods = templates_dict["input_path_neighbourhoods"]
        export_date = templates_dict["export_date"]
        output_path = templates_dict["output_path"]
        geopandas_kwargs = templates_dict["geopandas_kwargs"]

        logger.info(f"Reading neighbourhoods from {input_path_neighbourhoods}")
        logger.info(f"Reading listings from {input_path_listings}")

        listings_gdf = gpd.read_file(input_path_listings, **geopandas_kwargs)
        neighbourhoods_gdf = gpd.read_file(input_path_neighbourhoods, **geopandas_kwargs)

        logger.info(f"Ranking neighbourhoods based on number of listings")

        sj = neighbourhoods_gdf.sjoin(listings_gdf, how="inner", predicate="intersects")
        sj_count = sj.groupby("neighbourhood_left")["id_right"].nunique().reset_index()
        sj_count["export_date"] = export_date

        logger.info("Number of Airbnb listings based on neighbourhoods:")
        logger.info(sj_count.nlargest(n=5, columns="id_right", keep="all"))

        try:
            pd.read_csv(output_path)
            sj_count.to_csv(output_path, mode='a', header=False, index=False)
        except FileNotFoundError:
            sj_count.to_csv(output_path, mode='w', header=True, index=False)


    fetch_data_tasks = []
    rank_data_tasks = []

    for export_date in export_dates:
        task_id_count = f"count_listings_for_{export_date}"
        ranking_neighbourhoods = PythonOperator(
            task_id=task_id_count,
            python_callable=_rank_neighbourhoods,
            templates_dict={
                "input_path_listings": f"{settings.DATA_PATH}/listings_{export_date}.json",
                "input_path_neighbourhoods": f"{settings.DATA_PATH}/neighbourhoods_{export_date}.json",
                "export_date": export_date,
                "output_path": f"{settings.DATA_PATH}/rankings.csv",
                "geopandas_kwargs": {"driver": "GeoJSON", "crs": "EPSG:4326", "encoding": "utf-8", "index": True}
            },
        )
        rank_data_tasks.append(ranking_neighbourhoods)

        for endpoint in ["neighbourhoods", "listings"]:
            task_id = f"export_{endpoint}_{export_date}_using_custom_hook"
            fetch_operator = PythonOperator(
                task_id=task_id,
                python_callable=_fetch_data,
                op_kwargs={"conn_id": "airbnbapi"},
                templates_dict={
                    "endpoint": endpoint,
                    "start_date": export_date,
                    "out_path": f"{settings.DATA_PATH}/{endpoint}_{export_date}.json"
                },
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
