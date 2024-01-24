import os
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from custom.hooks import AirbnbApiHook


class AirbnbApiSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, conn_id, export_date, *args, **kwargs):
        super(AirbnbApiSensor, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.export_date = export_date

    def poke(self, context):
        # Check if there are listings or neighborhoods for the specified export date
        listings_exist = self.check_api_fetch("listings")
        neighborhoods_exist = self.check_api_fetch("neighbourhoods")

        # Return True if both listings and neighborhoods exist
        return listings_exist and neighborhoods_exist

    def check_api_fetch(self, endpoint):
        hook = AirbnbApiHook(self.conn_id)
        try:
            # Attempt to fetch data for the specified endpoint and export date
            data = hook.get_data(endpoint=endpoint, start_date=self.export_date, batch_size=100)

            # Check if any data was retrieved
            self.log.info(
                f"Found data for {self.export_date} to {endpoint}, continuing!"
            )
            if data:
                return True
        except StopIteration as e:
            # Handle exceptions as needed (e.g., log the error)
            self.log.exception(f"Error checking data existence for endpoint {endpoint}: {str(e)}")
            return False


class JsonDataSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, *args, **kwargs):
        super(JsonDataSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath

    def poke(self, context):
        # Check if the file exists
        self.log.info(f"Checking if file exists: {self.filepath}...")
        return os.path.exists(self.filepath)

