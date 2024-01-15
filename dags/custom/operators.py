import os
import json

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from hooks import AirbnbApiHook


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
            data = list(
                hook.get_data(
                    endpoint=self.endpoint, start_date=self.export_date, batch_size=self.batch_size
                )
            )
            self.log.info(f"Fetched {len(data)} data")
        finally:
            hook.close()

        self.log.info(f"Writing data to {self.out_path}")

        output_dir = os.path.dirname(self.out_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self.out_path, "w") as file_:
            json.dump(data, fp=file_)
