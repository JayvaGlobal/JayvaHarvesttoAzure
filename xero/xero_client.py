import requests

from .auth import get_valid_access_token

API_BASE = "https://api.xero.com/api.xro/2.0"


def get_headers(access_token: str, tenant_id: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json",
    }


def get_headers_for_connection(connection_name: str):
    access_token, tenant_id = get_valid_access_token(connection_name)
    return get_headers(access_token, tenant_id)


def get_paged(
    endpoint: str,
    connection_name: str = None,
    access_token: str = None,
    tenant_id: str = None,
    page_size: int = 100,
    extra_params: dict | None = None,
):
    """
    Pull a paged Xero endpoint.

    Preferred usage:
        get_paged("Contacts", connection_name="xero_<tenantid>")

    Backwards-compatible usage:
        get_paged("Contacts", access_token=token, tenant_id=tenant_id)
    """

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

        response = requests.get(
            url,
            headers=get_headers(access_token, tenant_id),
            params=params,
            timeout=120,
        )
        response.raise_for_status()
        data = response.json()

        payload = (
            data.get(endpoint)
            or data.get(endpoint.capitalize())
            or data.get(endpoint.upper())
            or []
        )

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
    """
    Pull a single-response Xero endpoint that is not paged.

    Preferred usage:
        get_one("Organisation", connection_name="xero_<tenantid>")
    """

    if connection_name:
        access_token, tenant_id = get_valid_access_token(connection_name)

    if not access_token or not tenant_id:
        raise ValueError("Provide either connection_name or both access_token and tenant_id")

    url = f"{API_BASE}/{endpoint}"

    response = requests.get(
        url,
        headers=get_headers(access_token, tenant_id),
        params=extra_params or {},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()
