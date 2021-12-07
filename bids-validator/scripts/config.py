import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache

SRV_NAMESPACE = os.environ.get("APP_NAME", "service_dataset")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")
CONFIG_CENTER_BASE_URL = os.environ.get("CONFIG_CENTER_BASE_URL", "NOT_SET")


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        return vault_factory(CONFIG_CENTER_BASE_URL)

def vault_factory(config_center) -> dict:
    url = config_center + \
        "/v1/utility/config/{}".format(SRV_NAMESPACE)
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']

class Settings(BaseSettings):
    port: int = 5081
    host: str = "127.0.0.1"
    env: str = ""
    namespace: str = ""

    MINIO_OPENID_CLIENT: str = ""
    MINIO_ENDPOINT: str = ""
    MINIO_HTTPS: str = ""
    KEYCLOAK_URL: str = ""
    KEYCLOAK_MINIO_SECRET: str 

    # temp path
    TEMP_DIR = ""
    DATA_OPS_UTIL: str = ""
    DATA_OPS_UT_V2: str = ""

    DATASET_SERVICE: str = ""
    QUEUE_SERVICE: str = ""

    RDS_DBNAME: str = ""
    RDS_HOST: str = ""
    RDS_USER: str = ""
    RDS_PWD: str = ""

    NEO4J_SERVICE: str = ""

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
        extra = Extra.allow

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings
        ):
            return (
                load_vault_settings,
                env_settings,
                init_settings,
                file_secret_settings
            )



@lru_cache()
def get_settings():
    settings =  Settings()
    return settings


class ConfigClass(object):
    settings = get_settings()

    version = "0.2.0"
    disk_namespace = settings.namespace

    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = (settings.MINIO_HTTPS == "True")
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    KEYCLOAK_MINIO_SECRET = settings.KEYCLOAK_MINIO_SECRET

    DATA_OPS_UT = f"{settings.DATA_OPS_UTIL}/v1/"
    DATA_OPS_UT_V2 = f"{settings.DATA_OPS_UTIL}/v2/"
    DATASET_SERVICE = f"{settings.DATASET_SERVICE}/v1"
    QUEUE_SERVICE = f"{settings.QUEUE_SERVICE}/v1/"

    POSTGREL_DB = settings.RDS_DBNAME
    POSTGREL_HOST = settings.RDS_HOST
    POSTGREL_USER = settings.RDS_USER
    POSTGREL_PWD = settings.RDS_PWD

    NEO4J_SERVICE = settings.NEO4J_SERVICE + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = settings.NEO4J_SERVICE + "/v2/neo4j/"
