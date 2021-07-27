import os


class ConfigClass(object):

    def __init__(self, env):
        self.MINIO_OPENID_CLIENT = "react-app"
        # minio config
        self.MINIO_OPENID_CLIENT = "react-app"
        self.MINIO_ENDPOINT = "minio.minio:9000"
        self.MINIO_HTTPS = False
        self.KEYCLOAK_URL = "http://keycloak.utility:8080"
        self.MINIO_ACCESS_KEY = "indoc-minio"
        self.MINIO_SECRET_KEY = "Trillian42!"

        if env == "test":
            # minio config
            self.MINIO_ENDPOINT = "10.3.7.220"
            self.MINIO_HTTPS = False
            self.KEYCLOAK_URL = "http://10.3.7.220" # for local test ONLY




