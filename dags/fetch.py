import logging
import os
import json
import requests
from requests import Session
import geopandas as gpd
import pandas as pd
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from geojson import FeatureCollection

from config import settings

os.environ["no_proxy"] = "*"
logger = logging.getLogger(__name__)

# Airbnb API credentials
credentials = {"username": f"{settings.API_USER}", "password": f"{settings.API_PASSWORD}"}
print(credentials)

# List of dates to export data for
export_dates = ["2023-09-03", "2023-06-05", "2023-03-09"]


def _get_session():
    """Build a request library Session and base url."""
    session: Session = requests.Session()
    base_url: str = f"{settings.AIRBNB_SCHEMA}://{settings.AIRBNB_HOST}:{settings.AIRBNB_PORT}"
    return session, base_url


def get_token(session: Session, base_url: str, credentials: dict):
    """Authenticate to the Airbnb listings API and fetch the token."""
    login_response = session.post(
        f"{base_url}/login/",
        data=credentials
    )
    login_response.raise_for_status()
    if login_response.status_code == 200:
        token = login_response.json().get("access_token")
        return token


def get_data(start_date: str, endpoint: str, batch_size=100):
    api_session, api_base_url = _get_session()
    token = get_token(api_session, api_base_url, credentials)

    pages = []
    for page in get_with_pagination(
        session=api_session,
        base_url=f"{api_base_url}/{endpoint}/",
        token=token,
        params={"query_date": start_date},
        batch_size=batch_size
    ):
        pages.append(page)

    return pages



def get_with_pagination(session: Session, base_url: str, token: str, params, batch_size=100):
    """
    Fetches records using a GET request with given URL/params,
    taking pagination into account.
    """
    offset = 0
    total = None
    while total is None or offset < total:
        response = session.get(
            base_url,
            headers={"Authorization": f"Bearer {token}"},
            params={
                **params,
                **{"offset": offset, "limit": batch_size}
            }
        )
        response.raise_for_status()
        response_json = response.json()

        yield response_json["results"]

        offset += batch_size
        total = response_json["total"]


with DAG(
        dag_id='API_to_JSON_file',
        description="Fetches data from the Airbnb API using the Python Operator.",
        start_date=airflow.utils.dates.days_ago(1),
        schedule=None,
        catchup=False,
        default_args={'provide_context': True}
) as dag:
    def _fetch_data(templates_dict: dict, batch_size=100, **_):
        endpoint = templates_dict["endpoint"]
        export_date = templates_dict["export_date"]
        out_path = templates_dict["out_path"]

        logger.info(f"Fetching data from Airbnb API with endpoint: {endpoint}")
        logger.info(f"Fetching data from {export_date}")

        data_pages = get_data(start_date=export_date, batch_size=batch_size, endpoint=endpoint)

        # Flatten the list of features from all pages
        features = [feature for page in data_pages for feature in page.get("features", [])]

        # Create a GeoJSON FeatureCollection
        feature_collection = FeatureCollection(features)

        logger.info(f"Fetched {len(features)} features from Airbnb API")

        logger.info(f"Writing data to {out_path}")

        with open(out_path, "w") as f:
            json.dump(feature_collection, f, indent=4)


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
                "input_path_listings": f"/Users/csunderliklaszlo/Dev/sandbox/insideairbnb/data/listings_{export_date}.json",
                "input_path_neighbourhoods": f"/Users/csunderliklaszlo/Dev/sandbox/insideairbnb/data/neighbourhoods_{export_date}.json",
                "export_date": export_date,
                "output_path": f"/Users/csunderliklaszlo/Dev/sandbox/insideairbnb/data/rankings.csv",
                "geopandas_kwargs": {"driver": "GeoJSON", "crs": "EPSG:4326", "encoding": "utf-8", "index": True}
            },
        )
        rank_data_tasks.append(ranking_neighbourhoods)

        for endpoint in ["neighbourhoods", "listings"]:
            task_id_export = f"export_{endpoint}_{export_date}"
            fetch_neighbourhoods = PythonOperator(
                task_id=task_id_export,
                python_callable=_fetch_data,
                templates_dict={
                    "endpoint": endpoint,
                    "export_date": export_date,
                    "out_path": f"/Users/csunderliklaszlo/Dev/sandbox/insideairbnb/data/{endpoint}_{export_date}.json"
                },
            )
            fetch_data_tasks.append(fetch_neighbourhoods)

    # Set task dependencies
    # Set task dependencies between fetch_data_tasks and rank_data_tasks
    for i in range(0, len(fetch_data_tasks), 2):
        # Connect two fetch_data_tasks to one rank_data_task
        fetch_data_tasks[i] >> rank_data_tasks[i // 2]
        fetch_data_tasks[i + 1] >> rank_data_tasks[i // 2]

    # Set task dependencies within fetch_data_tasks
    for i in range(2, len(fetch_data_tasks)):
        fetch_data_tasks[i - 2] >> fetch_data_tasks[i]
