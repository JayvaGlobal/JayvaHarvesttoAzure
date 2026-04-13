import requests
from .auth import get_valid_access_token

API_BASE = "https://api.xero.com/api.xro/2.0"


def get_headers(access_token: str, tenant_id: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }


def get_paged(endpoint: str, connection_name: str, page_size: int = 100, extra_params: dict | None = None):
    access_token, tenant_id = get_valid_access_token(connection_name)

    page = 1
    rows = []

    while True:
        url = f"{API_BASE}/{endpoint}"
        params = {"page": page}

        if extra_params:
            params.update(extra_params)

        response = requests.get(
            url,
            headers=get_headers(access_token, tenant_id),
            params=params,
            timeout=120,
        )
        response.raise_for_status()

        data = response.json()
        payload = data.get(endpoint) or data.get(endpoint.capitalize()) or []

        if not payload:
            break

        rows.extend(payload)

        if len(payload) < page_size:
            break

        page += 1

    return rows
