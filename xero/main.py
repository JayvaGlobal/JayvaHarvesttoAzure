from .config import XERO_CLIENT_ID, XERO_CLIENT_SECRET, XERO_REFRESH_TOKEN
from .auth import refresh_access_token, get_connections

def main():
    print("CLIENT ID loaded:", bool(XERO_CLIENT_ID))
    print("CLIENT SECRET loaded:", bool(XERO_CLIENT_SECRET))
    print("REFRESH TOKEN loaded:", bool(XERO_REFRESH_TOKEN))

    token_data = refresh_access_token()
    access_token = token_data["access_token"]

    connections = get_connections(access_token)

    print("Connected tenants:")
    for c in connections:
        print(c["tenantName"], c["tenantId"])

if __name__ == "__main__":
    main()
