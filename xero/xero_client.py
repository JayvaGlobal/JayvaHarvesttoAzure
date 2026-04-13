import time
import requests
from .auth import get_valid_access_token

API_BASE = "https://api.xero.com/api.xro/2.0"


def get_headers(access_token: str, tenant_id: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }


def safe_get(url: str, headers: dict, params: dict | None = None, timeout: int = 120):
    response = requests.get(url, headers=headers, params=params or {}, timeout=timeout)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        wait_seconds = int(retry_after) if retry_after and retry_after.isdigit() else 65
        time.sleep(wait_seconds)
        response = requests.get(url, headers=headers, params=params or {}, timeout=timeout)

    response.raise_for_status()
    return response


def get_paged(
    endpoint: str,
    connection_name: str = None,
    access_token: str = None,
    tenant_id: str = None,
    page_size: int = 100,
    extra_params: dict | None = None,
):
    if connection_name:
        access_token, tenant_id = get_valid_access_token(connection_name)

    if not access_token or not tenant_id:
        raise ValueError("Provide either connection_name or both access_token and tenant_id")

    page = 1
    rows = []

    while True:
        url = f"{API_BASE}/{endpoint}"
        params = {"page": page}

        if extra_params:
            params.update(extra_params)

        response = safe_get(
            url,
            headers=get_headers(access_token, tenant_id),
            params=params,
            timeout=120,
        )

        data = response.json()
        payload = data.get(endpoint) or data.get(endpoint.capitalize()) or []

        if not payload:
            break

        rows.extend(payload)

        if len(payload) < page_size:
            break

        page += 1

    return rows


def get_one(
    endpoint: str,
    connection_name: str = None,
    access_token: str = None,
    tenant_id: str = None,
    extra_params: dict | None = None,
):
    if connection_name:
        access_token, tenant_id = get_valid_access_token(connection_name)

    if not access_token or not tenant_id:
        raise ValueError("Provide either connection_name or both access_token and tenant_id")

    url = f"{API_BASE}/{endpoint}"

    response = safe_get(
        url,
        headers=get_headers(access_token, tenant_id),
        params=extra_params or {},
        timeout=120,
    )

    return response.json()
