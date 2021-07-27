import requests
import xmltodict
from minio import Minio
from minio.commonconfig import Tags
import os
import time
import datetime
from minio.credentials.providers import ClientGrantsProvider
from minio.commonconfig import REPLACE, CopySource


class Minio_Client():

    def __init__(self, _config):
        # set config
        self._config = _config
        # # retrieve credential provide with tokens
        # c = self.get_provider()

        # self.client = Minio(
        #     self._config.MINIO_ENDPOINT,
        #     c.access_key,
        #     c.secret_key,
        #     session_token=c.session_token,
        #     credentials=c,
        #     secure=self._config.MINIO_HTTPS)


        # Temperary use the credential
        self.client = Minio(
            self._config.MINIO_ENDPOINT, 
            access_key=self._config.MINIO_ACCESS_KEY,
            secret_key=self._config.MINIO_SECRET_KEY,
            secure=self._config.MINIO_HTTPS)


    def _get_jwt(self):
        # first login with keycloak
        username = "admin"
        password = self._config.MINIO_TEST_PASS
        payload = {
            "grant_type": "password",
            "username": username,
            "password": password,
            "client_id": self._config.MINIO_OPENID_CLIENT,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        result = requests.post(
            self._config.KEYCLOAK_URL+"/vre/auth/realms/vre/protocol/openid-connect/token", data=payload, headers=headers)
        keycloak_access_token = result.json().get("access_token")
        return result.json()

    def get_provider(self):
        minio_http = ("https://" if self._config.MINIO_HTTPS else "http://") + \
            self._config.MINIO_ENDPOINT
        print(minio_http)
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider.retrieve()

    def copy_object(self, bucket, obj, source_bucket, source_obj):
        result = self.client.copy_object(
            bucket,
            obj,
            CopySource(source_bucket, source_obj),
        )
        return result

    def fput_object(self, bucket_name, object_name, file_path):
        result = self.client.fput_object(
            bucket_name,
            object_name,
            file_path
        )
        return result
