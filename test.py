
# class GeometryNeighbourhoods(BaseModel):
#     type: str
#     geometry: List[List[Tuple[float, float]]]
#
# # Example input data
# input_data = "SRID=4326;MULTIPOLYGON(((4.991669 52.324436,4.991755 52.324289,4.991828 52.324175,4.991894 52.324077,4.991952 52.323996,4.992036 52.32387,4.992109 52.323767,4.99217 52.323706,4.992597 6)))"
#
# # Extract the WKT string from the input data
# wkt_string = input_data.split(";")[1]
#
# # Split the WKT string to get the type and coordinates
# wkt_parts = wkt_string.split("(")
# geometry_type = wkt_parts[0].strip()
# coordinates_str = wkt_parts[1].split(")")[0].strip()
#
# # Convert the coordinates to a list of lists of tuples
# coordinates_list = [
#     [tuple(map(float, coord.split())) for coord in polygon.split(",")]
#     for polygon in coordinates_str.split("),(")
# ]
#
# # Create an instance of GeometryNeighbourhoods
# geometry_neighbourhood = GeometryNeighbourhoods(type=geometry_type, geometry=coordinates_list)
#
# print(geometry_neighbourhood)


import logging
import os
import json
import requests
from requests import Session
import geopandas as gpd
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

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

    return list(get_with_pagination(
        session=api_session,
        base_url=f"{api_base_url}/{endpoint}/",
        token=token,
        params={"query_date": start_date},
        batch_size=batch_size
    ))


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

        yield from response_json["results"]

        offset += batch_size
        total = response_json["total"]


#
# for export_date in export_dates:
#     for endpoint in ["neighbourhoods", "listings"]:
#         task_id = f"export_{endpoint}_{export_date}"
#
#         data = get_data(
#                 start_date=export_date, batch_size=100, endpoint=endpoint
#             )
#         data_list = list(data)
#
#     print("OK")
#     print(data)




geojson_data = open("data/listings_2023-09-03.json")

data = json.load(geojson_data)

print(data)

