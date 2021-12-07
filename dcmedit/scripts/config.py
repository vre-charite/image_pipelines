import os
import requests
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache

SRV_NAMESPACE = os.environ.get("APP_NAME", "all")
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
    env: str = "test"
    namespace: str = ""
    version = "0.2.0"

    # minio
    MINIO_OPENID_CLIENT: str
    MINIO_ENDPOINT: str
    MINIO_HTTPS: str
    KEYCLOAK_URL: str
    KEYCLOAK_VRE_SECRET: str
    KEYCLOAK_MINIO_SECRET: str
    DATA_OPS_UTIL: str

    DATA_OPS_GR: str
    NEO4J_SERVICE: str
    ENTITYINFO_SERVICE: str
    UTILITY_SERVICE: str
    PROVENANCE_SERVICE: str
    DATA_OPS_UTIL: str
    CATALOGUING_SERVICE: str
    KEYCLOAK_VRE_SECRET: str

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


@lru_cache(1)
def get_settings():
    settings =  Settings()
    return settings


class ConfigClass(object):
    settings = get_settings()

    version = "0.2.0"
    env = settings.env
    disk_namespace = settings.namespace

    # minio
    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = settings.MINIO_HTTPS == "True"
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    KEYCLOAK_VRE_SECRET = settings.KEYCLOAK_VRE_SECRET
    KEYCLOAK_MINIO_SECRET = settings.KEYCLOAK_MINIO_SECRET
    DATA_OPS_UT_V2 = settings.DATA_OPS_UTIL + "/v2/"

    # services
    DATA_OPS_GR = settings.DATA_OPS_GR + "/v1/"
    DATA_OPS_GR_V2 = settings.DATA_OPS_GR + "/v2/"
    NEO4J_SERVICE = settings.NEO4J_SERVICE + "/v1/neo4j/"
    NEO4J_SERVICE_V2 = settings.NEO4J_SERVICE + "/v2/neo4j/"
    ENTITY_INFO_SERVICE = settings.ENTITYINFO_SERVICE + "/v1/"
    UTILITY_SERVICE = settings.UTILITY_SERVICE
    PROVENANCE_SERVICE = settings.PROVENANCE_SERVICE + "/v1/"
    DATA_OPS_UT = settings.DATA_OPS_UTIL + "/v1/"
    DATA_OPS_UT_V2 = settings.DATA_OPS_UTIL + "/v2/"
    CATALOGUING_SERVICE = settings.CATALOGUING_SERVICE + "v1/"
    CATALOGUING_SERVICE_V2 = settings.CATALOGUING_SERVICE + "/v2/"
    KEYCLOAK_VRE_SECRET = settings.KEYCLOAK_VRE_SECRET

    COMMON_SERVICE = settings.UTILITY_SERVICE + "/v1/"