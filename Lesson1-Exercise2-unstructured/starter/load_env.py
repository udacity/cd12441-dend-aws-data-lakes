import os
import sys
from pathlib import Path


def load_env(env_path="/workspace/.env"):
    env_file = Path(env_path)
    if not env_file.exists():
        print(
            f"⚠️  No .env file found at {env_path}.\n"
            f"   AWS calls will fail without credentials.\n"
            f"   See the course setup page for how to create this file from\n"
            f"   the Cloud Resources tab (AWS access key, secret, session token).",
            file=sys.stderr,
        )
        return
    for line in env_file.read_text().splitlines():
        if line.strip() and not line.startswith("#") and "=" in line:
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip())
