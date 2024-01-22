import os
import json
from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from geojson import FeatureCollection

from custom.hooks import AirbnbApiHook


class AirbnbFetchDataOperator(BaseOperator):
    """
    Operator that fetches data from the Airbnb API.
    """
    @apply_defaults
    def __init__(
            self,
            conn_id,
            out_path,
            export_date,
            batch_size=100,
            endpoint="listings",
            **kwargs,
    ):
        super(AirbnbFetchDataOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self.out_path = out_path
        self.export_date = export_date
        self.batch_size = batch_size
        self.endpoint = endpoint

    def execute(self, context):
        hook = AirbnbApiHook(self._conn_id)

        try:
            self.log.info(
                f"Fetching data for {self.export_date}"
            )
            data_pages = hook.get_data(
                    endpoint=self.endpoint, start_date=self.export_date, batch_size=self.batch_size
                )
            # Flatten the list of features from all pages
            features = [feature for page in data_pages for feature in page.get("features", [])]

            # Create a GeoJSON FeatureCollection
            feature_collection = FeatureCollection(features)

            self.log.info(f"Fetched {len(features)} data")
        finally:
            hook.close()

        self.log.info(f"Writing data to {self.out_path}")

        output_dir = os.path.dirname(self.out_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self.out_path, "w") as file_:
            json.dump(feature_collection, fp=file_, indent=4)


class AirbnbRankNeighbourhoodsOperator(BaseOperator):
    """
    Operator that ranks neighbourhoods based on the number of listings.
    """

    @apply_defaults
    def __init__(
            self,
            input_path_listings,
            input_path_neighbourhoods,
            export_date,
            output_path,
            geopandas_kwargs,
            **kwargs,
    ):
        super(AirbnbRankNeighbourhoodsOperator, self).__init__(**kwargs)
        self.input_path_listings = input_path_listings
        self.input_path_neighbourhoods = input_path_neighbourhoods
        self.export_date = export_date
        self.output_path = output_path
        self.geopandas_kwargs = geopandas_kwargs

    def execute(self, context: Context) -> Any:
        import geopandas as gpd
        import pandas as pd

        self.log.info(f"Reading neighbourhoods from {self.input_path_neighbourhoods}")
        self.log.info(f"Reading listings from {self.input_path_listings}")

        listings_gdf = gpd.read_file(self.input_path_listings, **self.geopandas_kwargs)
        neighbourhoods_gdf = gpd.read_file(self.input_path_neighbourhoods, **self.geopandas_kwargs)

        self.log.info(f"Ranking neighbourhoods based on number of listings")

        sj = neighbourhoods_gdf.sjoin(listings_gdf, how="inner", predicate="intersects")
        sj_count = sj.groupby("neighbourhood_left")["id_right"].nunique().reset_index()
        sj_count["export_date"] = self.export_date

        self.log.info("Number of Airbnb listings based on neighbourhoods:")
        self.log.info(sj_count.nlargest(n=5, columns="id_right", keep="all"))

        try:
            pd.read_csv(self.output_path)
            sj_count.to_csv(self.output_path, mode='a', header=False, index=False)
        except FileNotFoundError:
            sj_count.to_csv(self.output_path, mode='w', header=True, index=False)

