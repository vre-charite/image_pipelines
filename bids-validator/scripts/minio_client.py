import requests
import xmltodict
from minio import Minio
from minio.commonconfig import Tags
import os
import time
import datetime
from minio.credentials.providers import ClientGrantsProvider
from minio.commonconfig import REPLACE, CopySource


class Minio_Client_():


    def __init__(self, _config, access_token, refresh_token):
        self._config = _config
        # preset the tokens for refreshing
        self.access_token = access_token
        self.refresh_token = refresh_token
        
        # retrieve credential provide with tokens
        c = self.get_provider()

        self.client = Minio(
            self._config.MINIO_ENDPOINT, 
            credentials=c,
            secure=self._config.MINIO_HTTPS)


    # function helps to get new token/refresh the token
    def _get_jwt(self):
        print("refresh token")
        payload = {
            "grant_type" : "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id":self._config.MINIO_OPENID_CLIENT,
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        # use http request to fetch from keycloak
        result = requests.post(self._config.KEYCLOAK_URL+"/vre/auth/realms/vre/protocol/openid-connect/token", data=payload, headers=headers)
        if result.status_code != 200:
            raise Exception("Token refresh failed with "+str(result.json()))

        self.access_token = result.json().get("access_token")
        self.refresh_token = result.json().get("refresh_token")

        jwt_object = result.json()
        # print(jwt_object)

        return jwt_object

    # use the function above to create a credential object in minio
    # it will use the jwt function to refresh token if token expired
    def get_provider(self):
        minio_http = ("https://" if self._config.MINIO_HTTPS else "http://") + self._config.MINIO_ENDPOINT
        # print(minio_http)
        provider = ClientGrantsProvider(
            self._get_jwt,
            minio_http,
        )

        return provider

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



class Minio_Client():

    def __init__(self, _config):
        # set config
        self._config = _config

        # Temperary use the credential
        self.client = Minio(
            self._config.MINIO_ENDPOINT, 
            access_key=self._config.MINIO_ACCESS_KEY,
            secret_key=self._config.MINIO_SECRET_KEY,
            secure=self._config.MINIO_HTTPS)


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
