from airflow.providers.http.hooks.http import HttpHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import json

class XHook(HttpHook):
    """
    Interage com a API do X (antigo Twitter).
    Responsável por realizar requisições autenticadas e lidar com paginação.
    """

    def __init__(self, end_time, start_time, query, conn_id=None):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.conn_id = conn_id or "x_default"
        super().__init__(http_conn_id=self.conn_id)

    def create_url(self):
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

        x_fields = (
            "tweet.fields=author_id,conversation_id,created_at,id,"
            "in_reply_to_user_id,public_metrics,lang,text"
        )
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        url_raw = (
            f"{self.base_url}/2/tweets/search/recent?"
            f"query={self.query}&{x_fields}&{user_fields}"
            f"&start_time={self.start_time}&end_time={self.end_time}"
        )

        return url_raw

    def conn_to_endpoint(self, url, session):
        request = requests.Request("GET", url)
        prep = session.prepare_request(request)
        self.log.info(f"Requisição: {url}")

        response = self.run_and_check(session, prep, {})
        return response

    def pagination(self, url_raw, session):
        list_json_response = []
        response = self.conn_to_endpoint(url_raw, session)
        json_response = response.json()
        list_json_response.append(json_response)

        cont = 1

        # Paginação (até 10 páginas por padrão)
        while "next_token" in json_response.get("meta", {}) and cont <= 10:
            next_token = json_response["meta"]["next_token"]
            url = f"{url_raw}&next_token={next_token}"
            response = self.conn_to_endpoint(url, session)
            json_response = response.json()
            list_json_response.append(json_response)
            cont += 1

        self.log.info(f"Total de páginas coletadas: {cont}")
        return list_json_response

    def run(self):
        session = self.get_conn()
        url_raw = self.create_url()
        return self.pagination(url_raw, session)


if __name__ == "__main__":

    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    start_time = (datetime.now() - timedelta(days=1)).strftime(TIMESTAMP_FORMAT)
    query = "datascience"

    hook = XHook(end_time, start_time, query)

    for pg in hook.run():
        print(json.dumps(pg, indent=4, sort_keys=True))