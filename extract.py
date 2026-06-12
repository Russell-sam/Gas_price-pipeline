import http.client
import json

def fetch_state_gas_prices(state):
    conn = http.client.HTTPSConnection("api.collectapi.com")

    headers = {
        "content-type": "application/json",
        "authorization": "apikey 5q3wWoOvVBnUsiFatLXzOR:634M26w8HB7UEVPCWgT7Sb",
    }

    conn.request("GET", f"/gasPrice/stateUsaPrice?state={state}", headers=headers)

    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
    data = data["result"]["cities"]
    conn.close()

    return data