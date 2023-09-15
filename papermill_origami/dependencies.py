from origami.clients.api import APIClient
from pydantic import BaseSettings


class Settings(BaseSettings):
    NOTEABLE_TOKEN: str
    NOTEABLE_API_URL: str = "https://app.noteable.io/gate/api"


def get_api_client():
    settings = Settings()
    return APIClient(
        authorization_token=settings.NOTEABLE_TOKEN, api_base_url=settings.NOTEABLE_API_URL
    )
