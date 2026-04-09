import requests
from config import XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REFRESH_TOKEN

TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"

def refresh_access_token():
    resp = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": XERO_REFRESH_TOKEN
        },
        auth=(XERO_CLIENT_ID, XERO_CLIENT_SECRET),
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()

def get_connections(access_token: str):
    headers = {"Authorization": f"Bearer {access_token}"}
    resp = requests.get(CONNECTIONS_URL, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()
