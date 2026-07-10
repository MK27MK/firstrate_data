from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="FIRSTRATE_")

    userid: str
    base_url: str = "https://firstratedata.com/api"


settings = Settings()  # type: ignore[call-arg]  # userid comes from env
