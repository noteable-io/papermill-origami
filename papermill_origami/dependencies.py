from origami.clients.api import APIClient
from pydantic import BaseSettings


class Settings(BaseSettings):
    token: str
    public_url: str = "https://app.noteable.io"
    api_url: str = "https://app.noteable.io/gate/api"
    timeout: int = 60
    # TODO: update this to papermill_origami once Gate
    #       accepts the new client type
    rtu_client_type: str = "origami"

    class Config:
        env_prefix = "noteable_"


_settings = None


def get_settings() -> Settings:
    global _settings

    if _settings is None:
        _settings = Settings()

    return _settings


_singleton_api_client = None


def get_api_client() -> APIClient:
    global _singleton_api_client

    settings = get_settings()
    if _singleton_api_client is None:
        _singleton_api_client = APIClient(
            authorization_token=settings.token,
            api_base_url=settings.api_url,
            timeout=settings.timeout,
            rtu_client_type=settings.rtu_client_type,
        )

    return _singleton_api_client
