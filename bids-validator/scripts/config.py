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

    MINIO_OPENID_CLIENT: str = ""
    MINIO_ENDPOINT: str = ""
    MINIO_HTTPS: str = ""
    KEYCLOAK_URL: str = ""
    MINIO_TEST_PASS: str = ""
    MINIO_ACCESS_KEY: str = ""
    MINIO_SECRET_KEY: str = ""

    # disk mounts
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"
    # download secret
    GM_PASSWORD: str = ""
    DOWNLOAD_TOKEN_EXPIRE_AT = 5
    # temp path
    TEMP_DIR = ""
    DATA_OPS_UTIL: str = ""
    # Redis Service
    REDIS_HOST: str = ""
    REDIS_PORT: str = ""
    REDIS_DB: str = ""
    REDIS_PASSWORD: str = ""

    DATASET_SERVICE: str = ""
    DOWNLOAD_SERVICE_VRE: str = ""
    QUEUE_SERVICE: str = ""

    RDS_DBNAME: str = ""
    RDS_HOST: str = ""
    RDS_USER: str = ""
    RDS_PWD: str = ""

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
    disk_namespace = os.environ.get('namespace')

    MINIO_OPENID_CLIENT = settings.MINIO_OPENID_CLIENT
    MINIO_ENDPOINT = settings.MINIO_ENDPOINT
    MINIO_HTTPS = (settings.MINIO_HTTPS == "True")
    KEYCLOAK_URL = settings.KEYCLOAK_URL
    MINIO_TEST_PASS = settings.MINIO_TEST_PASS
    MINIO_ACCESS_KEY = settings.MINIO_ACCESS_KEY
    MINIO_SECRET_KEY = settings.MINIO_SECRET_KEY

    # disk mounts
    NFS_ROOT_PATH = settings.NFS_ROOT_PATH
    VRE_ROOT_PATH = settings.VRE_ROOT_PATH
    # download secret
    DOWNLOAD_KEY = settings.GM_PASSWORD
    DOWNLOAD_TOKEN_EXPIRE_AT = settings.DOWNLOAD_TOKEN_EXPIRE_AT
    # temp path
    TEMP_DIR = settings.TEMP_DIR
    DATA_OPS_UT = f"{settings.DATA_OPS_UTIL}/v1/"
    # Redis Service
    REDIS_HOST = settings.REDIS_HOST
    REDIS_PORT = int(settings.REDIS_PORT)
    REDIS_DB = int(settings.REDIS_DB)
    REDIS_PASSWORD = settings.REDIS_PASSWORD

    DATASET_SERVICE = f"{settings.DATASET_SERVICE}/v1"
    DOWNLOAD_SERVICE = settings.DOWNLOAD_SERVICE_VRE
    QUEUE_SERVICE = f"{settings.QUEUE_SERVICE}/v1/"

    POSTGREL_DB = settings.RDS_DBNAME
    POSTGREL_HOST = settings.RDS_HOST
    POSTGREL_USER = settings.RDS_USER
    POSTGREL_PWD = settings.RDS_PWD
