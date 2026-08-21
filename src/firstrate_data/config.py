import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

DEFAULT_BASE_URL = "https://firstratedata.com/api"
_BASE_URL_KEY = "FIRSTRATE_BASE_URL"


class MissingSettingError(KeyError):
    pass


def _find_and_load_dotenv_cwd() -> None:
    load_dotenv(find_dotenv(usecwd=True))


def _raise_if_missing(env_variable: str) -> str:
    _find_and_load_dotenv_cwd()
    value = os.getenv(env_variable)
    if value:
        return value
    msg = f"set {env_variable} environment variable"
    raise MissingSettingError(msg)


# ----------------------------------------------------------------------
# Store config
# ----------------------------------------------------------------------


def firstrate_data_path() -> Path:
    return Path(_raise_if_missing("FIRSTRATE_DATA_PATH"))


# ----------------------------------------------------------------------
# Client config
# ----------------------------------------------------------------------


def firstrate_user_id() -> str:
    return _raise_if_missing("FIRSTRATE_USERID")


def base_url() -> str:
    """Return the vendor's API root."""
    # FIRSTRATE_BASE_URL lets a test point the client at a stand-in server.
    _find_and_load_dotenv_cwd()
    return os.getenv(_BASE_URL_KEY, DEFAULT_BASE_URL)
