import argparse
import os
from pathlib import Path

from kmb.mcp.server import Settings, create_server

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("transport", nargs="?", choices=["stdio", "http"], default="stdio")
    args = parser.parse_args()
    if args.transport == "stdio":
        create_server(Settings.from_env()).run(transport="stdio")
    else:
        import uvicorn

        from kmb.mcp.http import create_http_app

        cert = os.environ["KMB_MCP_TLS_CERT_FILE"]
        key = os.environ["KMB_MCP_TLS_KEY_FILE"]
        token_file = os.getenv("KMB_MCP_TOKEN_FILE")
        service_token = Path(token_file).read_text(encoding="utf-8").strip() if token_file else ""
        if token_file and not service_token:
            raise ValueError("KMB_MCP_TOKEN_FILE must point to a nonempty research-token file")
        app = create_http_app(
            os.getenv("KMB_MCP_BACKEND_URL", "http://backend:8800"),
            os.getenv("KMB_MCP_PUBLIC_URL", "https://node003:8801"),
            service_token,
        )
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8801,
            ssl_certfile=cert,
            ssl_keyfile=key,
            proxy_headers=False,
        )
