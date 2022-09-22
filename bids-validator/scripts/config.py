# Copyright 2022 Indoc Research
# 
# Licensed under the EUPL, Version 1.2 or – as soon they
# will be approved by the European Commission - subsequent
# versions of the EUPL (the "Licence");
# You may not use this work except in compliance with the
# Licence.
# You may obtain a copy of the Licence at:
# 
# https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12
# 
# Unless required by applicable law or agreed to in
# writing, software distributed under the Licence is
# distributed on an "AS IS" basis,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied.
# See the Licence for the specific language governing
# permissions and limitations under the Licence.
# 

import os
from common import VaultClient
from requests.models import HTTPError
from pydantic import BaseSettings, Extra
from typing import Dict, Set, List, Any
from functools import lru_cache
from dotenv import load_dotenv

#load env var from local env file for local test
load_dotenv()
SRV_NAMESPACE = os.environ.get("APP_NAME", "service_dataset")
CONFIG_CENTER_ENABLED = os.environ.get("CONFIG_CENTER_ENABLED", "false")


def load_vault_settings(settings: BaseSettings) -> Dict[str, Any]:
    if CONFIG_CENTER_ENABLED == "false":
        return {}
    else:
        vc = VaultClient(os.getenv("VAULT_URL"), os.getenv("VAULT_CRT"), os.getenv("VAULT_TOKEN"))
        return vc.get_from_vault(SRV_NAMESPACE)


class Settings(BaseSettings):
    port: int = 5081
    host: str = "127.0.0.1"
    env: str = ""
    version: str = "0.2.0"
    namespace: str = ""

    MINIO_OPENID_CLIENT: str = ""
    MINIO_ENDPOINT: str = ""
    MINIO_HTTPS: str = ""
    KEYCLOAK_MINIO_SECRET: str 
    KEYCLOAK_ENDPOINT: str

    # temp path
    DATA_OPS_UT: str = ""
    DATA_OPS_UT_V2: str = ""

    DATASET_SERVICE: str = ""
    QUEUE_SERVICE: str = ""

    RDS_DBNAME: str = ""
    RDS_HOST: str = ""
    RDS_USER: str = ""
    RDS_PWD: str = ""
    SQL_DB_NAME: str = ""

    NEO4J_SERVICE_V1: str = ""
    NEO4J_SERVICE_V2: str = ""

    def __init__(self):
        super().__init__()
        self.MINIO_HTTPS = (self.MINIO_HTTPS == "True")
        self.DATA_OPS_UT = self.DATA_OPS_UTIL + "/v1/"
        self.DATA_OPS_UT_V2 = self.DATA_OPS_UTIL + "/v2/"
        self.DATASET_SERVICE += "/v1"
        self.QUEUE_SERVICE += "/v1/"
        self.NEO4J_SERVICE_V1 = self.NEO4J_SERVICE + "/v1/neo4j/"
        self.NEO4J_SERVICE_V2 = self.NEO4J_SERVICE + "/v2/neo4j/"

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

ConfigClass = Settings()
