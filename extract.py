import http.client
import json
import os
from dotenv import load_dotenv

load_dotenv()


def fetch_state_gas_prices(state):
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
        "content-type": "application/json",
        'authorization': os.getenv('API_KEY')
    }

    conn.request("GET", f"/gasPrice/stateUsaPrice?state={state}", headers=headers)

    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
    data = data["result"]["cities"]
    conn.close()

    return data