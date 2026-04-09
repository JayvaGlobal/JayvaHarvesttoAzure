import requests

API_BASE = "https://api.xero.com/api.xro/2.0"

def get_headers(access_token: str, tenant_id: str):
    return {
        "Authorization": f"Bearer {access_token}",
        "Xero-tenant-id": tenant_id,
        "Accept": "application/json"
    }

def get_paged(endpoint: str, access_token: str, tenant_id: str, page_size: int = 100):
    page = 1
    rows = []
    while True:
        url = f"{API_BASE}/{endpoint}"
        resp = requests.get(url, headers=get_headers(access_token, tenant_id), params={"page": page}, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        entity_name = endpoint
        payload = data.get(entity_name) or data.get(entity_name.capitalize()) or []
        if not payload:
            break
        rows.extend(payload)
        if len(payload) < page_size:
            break
        page += 1
    return rows
