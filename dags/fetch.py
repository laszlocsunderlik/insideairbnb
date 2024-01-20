import logging
import os
import json
import requests
from requests import Session
import geopandas as gpd
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
        start_date = templates_dict["start_date"]
        out_path = templates_dict["out_path"]

        logger.info(f"Fetching data from Airbnb API with endpoint: {endpoint}")
        logger.info(f"Fetching data from {start_date}")

        data_pages = get_data(start_date=start_date, batch_size=batch_size, endpoint=endpoint)

        # Flatten the list of features from all pages
        features = [feature for page in data_pages for feature in page.get("features", [])]

        # Create a GeoJSON FeatureCollection
        feature_collection = FeatureCollection(features)

        logger.info(f"Fetched {len(features)} features from Airbnb API")

        logger.info(f"Writing data to {out_path}")

        with open(out_path, "w") as f:
            json.dump(feature_collection, f, indent=4)


    def _rank_neighbourhoods(templates_dict: dict, **_):
        input_path = templates_dict["input_path"]
        output_path = templates_dict["output_path"]

        logger.info(f"Reading neighbourhoods from {input_path}")
        listings_gdf = gpd.read_file(input_path, engine="python")
        neighbourhoods_gdf = gpd.read_file(input_path, engine="python")

        logger.info(f"Ranking neighbourhoods based on number of listings")
        pass


    tasks = []

    for export_date in export_dates:
        for endpoint in ["neighbourhoods", "listings"]:
            task_id = f"export_{endpoint}_{export_date}"
            fetch_neighbourhoods = PythonOperator(
                task_id=task_id,
                python_callable=_fetch_data,
                templates_dict={
                    "endpoint": endpoint,
                    "start_date": export_date,
                    "out_path": f"/Users/csunderliklaszlo/Dev/sandbox/insideairbnb/data/{endpoint}_{export_date}.json"
                },
                dag=dag,

            )
            tasks.append(fetch_neighbourhoods)



    # Set task dependencies
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
