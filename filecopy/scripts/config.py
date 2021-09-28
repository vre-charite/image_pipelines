import os
import requests
from requests.models import HTTPError

CONFIG_CENTER = "http://common.utility:5062"
_config = None

def config_singleton(env="test"):
    global _config
    if _config:
        return _config
    else:
        set_config(config_factory(env))
        return _config

def set_config(config):
    global _config
    _config = config

def config_factory(env: str):
    return ConfigClass(env)

def vault_factory(config_center) -> dict:
    url = config_center + "/v1/utility/config/all"
    config_center_respon = requests.get(url)
    if config_center_respon.status_code != 200:
        raise HTTPError(config_center_respon.text)
    return config_center_respon.json()['result']

class ConfigClass(object):

    def __init__(self, env):
        global CONFIG_CENTER
        vault = vault_factory(CONFIG_CENTER)
        self.env = vault['ENV']
        self.disk_namespace = os.environ.get('namespace')
        self.version = "0.2.0"

        # minio
        self.MINIO_OPENID_CLIENT = vault['MINIO_OPENID_CLIENT']
        self.MINIO_ENDPOINT = vault['MINIO_ENDPOINT']
        self.MINIO_HTTPS = vault['MINIO_HTTPS'] == True
        self.KEYCLOAK_URL = vault['KEYCLOAK_URL']
        self.MINIO_TEST_PASS = vault['MINIO_TEST_PASS']
        self.KEYCLOAK_VRE_SECRET = vault['KEYCLOAK_VRE_SECRET']
        self.MINIO_ACCESS_KEY = "indoc-minio" # deprecate
        self.MINIO_SECRET_KEY = "Trillian42!" # deprecate

        # disk mounts
        self.NFS_ROOT_PATH = "./"
        self.VRE_ROOT_PATH = "/vre-data"
        self.ROOT_PATH = {
            "vre": "/vre-data"
        }.get(os.environ.get('namespace'), "/data/vre-storage")

        # download secret
        self.DOWNLOAD_KEY = "indoc101"
        self.DOWNLOAD_TOKEN_EXPIRE_AT = 5

        # temp path
        self.TEMP_DIR = ""
        self.DATA_OPS_UT = vault["DATA_OPS_UTIL"] + "/v1/"

        # Redis Service
        self.REDIS_HOST = vault['RDS_HOST']
        self.REDIS_PORT = vault['RDS_PORT']
        self.REDIS_DB = vault['RDS_PWD']
        self.REDIS_PASSWORD = vault['RDS_PWD'] # deprecate

        # services
        self.DATA_OPS_GR = vault["DATA_OPS_GR"] + "/v1/"
        self.DATA_OPS_GR_V2 = vault["DATA_OPS_GR"] + "/v2/"
        self.NEO4J_SERVICE = vault["NEO4J_SERVICE"] + "/v1/neo4j/"
        self.NEO4J_SERVICE_V2 = vault["NEO4J_SERVICE"] + "/v2/neo4j/"
        self.ENTITY_INFO_SERVICE = vault["ENTITYINFO_SERVICE"] + "/v1/"
        self.UTILITY_SERVICE = vault["UTILITY_SERVICE"]
        self.PROVENANCE_SERVICE = vault["PROVENANCE_SERVICE"]+"/v1/"
        self.DATA_OPS_UT_V2 = vault["DATA_OPS_UTIL"] + "/v2/"
        self.CATALOGUING_SERVICE = vault["CATALOGUING_SERVICE"] + "v1/"
        self.CATALOGUING_SERVICE_V2 = vault["CATALOGUING_SERVICE"] + "/v2/"
        self.KEYCLOAK_VRE_SECRET = vault["KEYCLOAK_VRE_SECRET"]

        # envs
        self.copied_with_approval = 'copied-to-core'


    