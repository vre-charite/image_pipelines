import requests
from minio import Minio
import os
import time
import datetime

from config import ConfigClass
from minio.credentials.providers import ClientGrantsProvider


class Minio_Client():

    def __init__(self, env):
        self.config = ConfigClass(env)
        # retrieve credential provide with tokens
        # c = self.get_provider()

        # self.client = Minio(
        #     self.config.MINIO_ENDPOINT,
        #     credentials=c,
        #     secure=self.config.MINIO_HTTPS)

        # Temperary use the credential
        self.client = Minio(
            self.config.MINIO_ENDPOINT, 
            access_key=self.config.MINIO_ACCESS_KEY,
            secret_key=self.config.MINIO_SECRET_KEY,
            secure=self.config.MINIO_HTTPS)

    # function helps to get new token/refresh the token
    def _get_jwt(self):
        username = "admin"
        password = self.config.MINIO_TEST_PASS
        payload = {
            "grant_type": "password",
            "username": username,
            "password": password, 
            "client_id": self.config.MINIO_OPENID_CLIENT,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # use http request to fetch from keycloak
        result = requests.post(self.config.KEYCLOAK_URL+"/vre/auth/realms/vre/protocol/openid-connect/token", data=payload, headers=headers)
        keycloak_access_token = result.json().get("access_token")
        return result.json()

    # use the function above to create a credential object in minio
    # it will use the jwt function to refresh token if token expired
    def get_provider(self):
        minio_http = ("https://" if self.config.MINIO_HTTPS else "http://") + self.config.MINIO_ENDPOINT
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )
        return provider
