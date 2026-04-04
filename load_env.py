import os
from pathlib import Path


def load_env(env_path="/workspace/.env"):
    env_file = Path(env_path)
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                key, val = line.split("=", 1)
                os.environ.setdefault(key.strip(), val.strip())
