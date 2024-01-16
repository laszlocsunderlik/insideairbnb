import os
import requests
from airflow.hooks.base import BaseHook

os.environ["no_proxy"] = "*"  # Disable proxy for local development


class AirbnbApiHook(BaseHook):
    def __init__(self, conn_id, retry=3):
        super().__init__()
        self._conn_id = conn_id
        self._retry = retry

        self.session = None
        self.base_url = None
        self.credentials = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_session(self):
        """
        Returns the connection used by the hook for querying data.
        Should in principle not be used directly.
        """
        if self.session is None:
            config = self.get_connection(self._conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection {self._conn_id}")

            schema = config.schema
            host = config.host
            port = config.port

            self.session = requests.Session()
            self.base_url = f"{schema}://{host}:{port}"
            self.credentials = {"username": f"{config.login}", "password": f"{config.password}"}

        return self.session, self.base_url, self.credentials

    def close(self):
        """Closes any active session."""
        if self.session:
            self.session.close()
        self.session = None
        self.base_url = None

    def connect_to_api(self):
        """Authenticate to the Airbnb listings API and fetch the token."""
        login_response = self.session.post(
            f"{self.base_url}/login/",
            data=self.credentials
        )
        login_response.raise_for_status()
        if login_response.status_code == 200:
            token = login_response.json().get("access_token")
            return token

    def get_data(self, endpoint, start_date: str = None, batch_size: int = 100):
        """
        Fetches records using a GET request with given URL/params.
        :param endpoint: endpoint to fetch data from (neighbourhoods or listings)
        :param start_date: starting date to fetch records from
        :param batch_size: page size
        :return:
        """

        yield from self.get_with_pagination(
            endpoint=endpoint,
            params={"query_date": start_date},
            batch_size=batch_size
        )

    def get_with_pagination(self, endpoint, params, batch_size=100):
        """
        Fetches records using a GET request with given URL/params,
        taking pagination into account.
        """

        session, api_base_url, credentials = self.get_session()
        token = self.connect_to_api()

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(
                api_base_url + "/" + endpoint,
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

