"""CLI: ``serve`` (uvicorn), ``create-admin`` (bootstrap/reset an admin), ``gen-vault-key``."""

from __future__ import annotations

import argparse
import getpass
import logging
import os
import sys

from sqlalchemy import select


def _create_admin(username: str, password: str | None) -> int:
    from kmb.backend.security import check_password_policy, hash_password
    from kmb.db import init_db, make_engine, session_factory
    from kmb.db.models import User

    password = password or os.environ.get("KMB_ADMIN_PASSWORD") or getpass.getpass("password: ")
    if err := check_password_policy(password):
        print(err, file=sys.stderr)
        return 2
    engine = make_engine()
    init_db(engine)
    with session_factory(engine)() as db:
        user = db.scalar(select(User).where(User.username == username))
        if user is None:
            db.add(User(username=username, password_hash=hash_password(password), role="admin"))
            print(f"created admin {username}")
        else:
            user.password_hash = hash_password(password)
            user.role = "admin"
            user.is_active = True
            print(f"reset admin {username}")
        db.commit()
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m kmb.backend")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("serve")
    ca = sub.add_parser("create-admin")
    ca.add_argument("username")
    ca.add_argument("--password", help="defaults to $KMB_ADMIN_PASSWORD or a prompt")
    sub.add_parser("gen-vault-key")
    args = p.parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("KMB_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    if args.cmd == "gen-vault-key":
        from kmb.backend.security import Vault

        print(Vault.generate_key_hex())
        return 0
    if args.cmd == "create-admin":
        return _create_admin(args.username, args.password)

    import uvicorn

    from kmb.backend.app import create_app
    from kmb.backend.settings import BackendSettings

    settings = BackendSettings.from_env()
    uvicorn.run(
        create_app(settings), host=settings.host, port=settings.port, proxy_headers=False, log_level="info"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
