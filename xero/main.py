from .auth import refresh_access_token, get_connections

def main():
    token_data = refresh_access_token()
    access_token = token_data["access_token"]

    connections = get_connections(access_token)

    print("Connected tenants:")
    for c in connections:
        print(c["tenantName"], c["tenantId"])

if __name__ == "__main__":
    main()
