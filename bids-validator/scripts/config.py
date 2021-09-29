import os

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
    return {
        "dev": ConfigDev(),
        "staging": ConfigStaging(),
        "charite": ConfigProd(),
        "test": ConfigDev()
    }.get(env)


class ConfigDev():
    env = "dev"
    disk_namespace = os.environ.get('namespace')
    version = "0.2.0"
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "10.3.7.220"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://10.3.7.220"
    MINIO_TEST_PASS = "admin"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"

    # disk mounts
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"
    # download secret
    DOWNLOAD_KEY = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT = 5
    # temp path
    TEMP_DIR = ""
    DATA_OPS_UT = "http://10.3.7.239:5063/v1/"
    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = "5wCCMMC1Lk"

    DATASET_SERVICE = "http://10.3.7.209:5081/v1"
    DOWNLOAD_SERVICE = "http://10.3.7.235:5077"
    QUEUE_SERVICE = "http://10.3.7.214:6060/v1/"

    POSTGREL_DB = "INDOC_VRE"
    POSTGREL_HOST = "10.3.7.215"
    POSTGREL_USER = "postgres"
    POSTGREL_PWD = "postgres"


class ConfigStaging():
    env = "staging"
    # minio setting
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "minio.minio:9000"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://keycloak.utility:8080"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"

    disk_namespace = os.environ.get('namespace')
    version = "0.2.0"
    # disk mounts
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"
    # download secret
    DOWNLOAD_KEY = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT = 5
    # temp path
    TEMP_DIR = ""
    DATA_OPS_UT = "http://dataops-ut.utility:5063/v1/"
    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = "8EH6QmEYJN"

    DATASET_SERVICE = "http://dataset.utility:5081/v1"
    DOWNLOAD_SERVICE = "http://download.vre:5077"
    QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"

    POSTGREL_DB = "INDOC_VRE"
    POSTGREL_HOST = "opsdb.utility"
    POSTGREL_USER = "postgres"
    POSTGREL_PWD = "postgres"


class ConfigProd():
    env = "charite"
    disk_namespace = os.environ.get('namespace')
    version = "0.2.0"

    # minio setting
    MINIO_OPENID_CLIENT = "react-app"
    MINIO_ENDPOINT = "minio.minio:9000"
    MINIO_HTTPS = False
    KEYCLOAK_URL = "http://keycloak.utility:8080"
    MINIO_ACCESS_KEY = "indoc-minio"
    MINIO_SECRET_KEY = "Trillian42!"

    # disk mounts
    NFS_ROOT_PATH = "/data/vre-storage"
    VRE_ROOT_PATH = "/vre-data"
    # download secret
    DOWNLOAD_KEY = "indoc101"
    DOWNLOAD_TOKEN_EXPIRE_AT = 5
    # temp path
    TEMP_DIR = ""
    DATA_OPS_UT = "http://dataops-ut.utility:5063/v1/"
    # Redis Service
    REDIS_HOST = "redis-master.utility"
    REDIS_PORT = 6379
    REDIS_DB = 0
    REDIS_PASSWORD = "o2x7vGQx6m"

    DATASET_SERVICE = "http://dataset.utility:5081/v1"
    DOWNLOAD_SERVICE = "http://download.greenroom:5077"
    QUEUE_SERVICE = "http://queue-producer.greenroom:6060/v1/"

    POSTGREL_DB = "INDOC_VRE"
    POSTGREL_HOST = "opsdb.utility"
    POSTGREL_USER = "indoc_vre"
    POSTGREL_PWD = "opsdb-jrjmfa9svvC"
