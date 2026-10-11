from kmb.mcp.server import Settings, create_server

if __name__ == "__main__":
    create_server(Settings.from_env()).run(transport="stdio")
