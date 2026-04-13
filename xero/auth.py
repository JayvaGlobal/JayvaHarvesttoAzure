import base64
from datetime import datetime, timezone

import requests

from .config import XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REDIRECT_URI
from .db import (
    get_engine,
    ensure_xero_oauth_table,
    save_xero_connection,
    load_xero_connection,
    update_xero_tokens,
)

TOKEN_URL = "https://identity.xero.com/connect/token"


def exchange_code_for_tokens(auth_code: str):
    creds = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    basic_auth = base64.b64encode(creds.encode()).decode()

    headers = {
        "Authorization": f"Basic {basic_auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": XERO_REDIRECT_URI,
    }

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=60)
    r.raise_for_status()
    return r.json()


def refresh_xero_token(refresh_token: str):
    creds = f"{XERO_CLIENT_ID}:{XERO_CLIENT_SECRET}"
    basic_auth = base64.b64encode(creds.encode()).decode()

    headers = {
        "Authorization": f"Basic {basic_auth}",
        "Content-Type": "application/x-www-form-urlencoded",
    }

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    r = requests.post(TOKEN_URL, headers=headers, data=data, timeout=60)
    r.raise_for_status()
    return r.json()


def get_connections(access_token: str):
    r = requests.get(
        "https://api.xero.com/connections",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=60,
    )
    r.raise_for_status()
    return r.json()


def save_initial_tokens_from_code(auth_code: str):
    engine = get_engine()
    ensure_xero_oauth_table(engine)

    tokens = exchange_code_for_tokens(auth_code)
    connections = get_connections(tokens["access_token"])

    saved = []

    for c in connections:
        connection_name = f"xero_{c['tenantId']}"
        save_xero_connection(
            engine=engine,
            connection_name=connection_name,
            tenant_id=c["tenantId"],
            tenant_name=c["tenantName"],
            access_token=tokens["access_token"],
            refresh_token=tokens["refresh_token"],
            expires_in=tokens["expires_in"],
        )
        saved.append({
            "connection_name": connection_name,
            "tenant_name": c["tenantName"],
            "tenant_id": c["tenantId"],
        })

    return saved


def get_valid_access_token(connection_name: str):
    engine = get_engine()
    state = load_xero_connection(engine, connection_name)

    expires_at = state["access_token_expires_at"]
    if expires_at is not None and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    now_utc = datetime.now(timezone.utc)

    if expires_at is not None and expires_at > now_utc:
        return state["access_token"], state["tenant_id"]

    refreshed = refresh_xero_token(state["refresh_token"])

    update_xero_tokens(
        engine=engine,
        connection_name=connection_name,
        access_token=refreshed["access_token"],
        refresh_token=refreshed["refresh_token"],
        expires_in=refreshed["expires_in"],
    )

    return refreshed["access_token"], state["tenant_id"]
