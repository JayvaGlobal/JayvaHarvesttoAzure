import requests
from .config import XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REFRESH_TOKEN

TOKEN_URL = "https://identity.xero.com/connect/token"
CONNECTIONS_URL = "https://api.xero.com/connections"


def refresh_access_token():
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": XERO_REFRESH_TOKEN,
        },
        auth=(XERO_CLIENT_ID, XERO_CLIENT_SECRET),
        timeout=60,
    )

    print("Token response status:", response.status_code)
    print("Token response body:", response.text)

    response.raise_for_status()
    return response.json()


def get_connections(access_token: str):
    response = requests.get(
        CONNECTIONS_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )

    print("Connections response status:", response.status_code)
    print("Connections response body:", response.text)

    response.raise_for_status()
    return response.json()
