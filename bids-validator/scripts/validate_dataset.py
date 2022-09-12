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

import requests
import subprocess
import json
import time
import psycopg2
import argparse
from datetime import datetime
from config import ConfigClass
import traceback
import shutil
import os

from minio_client import Minio_Client_
from locks import recursive_lock, unlock_resource

TEMP_FOLDER = './dataset/'


def debug_message_sender(message: str):
    url = ConfigClass.DATA_OPS_UT + "files/actions/message"
    print(url)
    response = requests.post(url, json={
        "message": message,
        "channel": "pipelinewatch"
    })
    if response.status_code != 200:
        logger_info("code: " + str(response.status_code) +
                    ": " + response.text)
    return


def logger_info(message: str):
    debug_message_sender(message)
    print(message)


def parse_inputs() -> dict:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('-d', '--dataset-geid',
                        help='Dataset geid', required=True)
    parser.add_argument('-env', '--environment',
                        help='Environment', required=True)
    parser.add_argument('-refresh', '--refresh-token',
                        help='Refresh Token', required=True)
    parser.add_argument('-access', '--access-token',
                        help='Access Token', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def send_message(dataset_geid, status, bids_output) -> None:
    queue_url = ConfigClass.QUEUE_SERVICE + "broker/pub"
    post_json = {
        "event_type": "BIDS_VALIDATE_NOTIFICATION",
        "payload": {
            "status": status,         # INIT/RUNNING/FINISH/ERROR
            "dataset": dataset_geid,
            "payload": bids_output,
            "update_timestamp": time.time()
        },
        "binary": True,
        "queue": "socketio",
        "routing_key": "socketio",
        "exchange": {
            "name": "socketio",
            "type": "fanout"
        }
    }

    if status == "failed":
        post_json["payload"]["payload"] = None
        post_json["payload"]["error_msg"] = bids_output

    try:
        queue_res = requests.post(queue_url, json=post_json)
        if queue_res.status_code != 200:
            logger_info("code: " + str(queue_res.status_code) +
                        ": " + queue_res.text)
        logger_info("sent message to queue")
        return
    except Exception as e:
        logger_info(f"Failed to send message to queue: {str(e)}")
        raise


def get_files_recursive(dataset_geid, folder_geid=None, all_files=None) -> list:
    if all_files is None:
        all_files = []

    query = {
        "page": 0,
        "page_size": 10000,
        "order_by": "create_time",
        "order_type": "desc"
    }
    if folder_geid:
        query["folder_geid"] = folder_geid

    try:
        resp = requests.get(ConfigClass.DATASET_SERVICE +
                            "/dataset/{}/files".format(dataset_geid), params=query)
        for node in resp.json()["result"]['data']:
            if "File" in node["labels"]:
                all_files.append(node["location"])
            else:
                get_files_recursive(
                    dataset_geid, node['global_entity_id'], all_files=all_files)
        return all_files
    except Exception as e:
        logger_info(f"Error when get files: {str(e)}")
        raise


def download_from_minio(files_locations, auth_token) -> None:
    mc = Minio_Client_(auth_token["at"], auth_token["rt"])
    logger_info("========Minio_Client Initiated========")

    try:
        for file_location in files_locations:
            minio_path = file_location.split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))

            mc.client.fget_object(bucket, obj_path, TEMP_FOLDER + obj_path)
        logger_info("========Minio_Client download finished========")

    except Exception as e:
        logger_info(f"Error when download data from minio: {str(e)}")
        raise


def getProcessOutput() -> None:
    f = open("result.txt", "w")
    try:
        subprocess.run(
            ['bids-validator', TEMP_FOLDER + 'data', '--json'], universal_newlines=True, stdout=f)
    except Exception as e:
        logger_info(f"BIDS validate fail: {str(e)}")
        raise


def read_result_file() -> str:
    f = open("result.txt", "r")
    output = f.read()
    return output


def main():
    logger_info('Vault url: ' + os.getenv("VAULT_URL"))
    environment = args.get('environment', 'test')
    logger_info('environment: ' + str(args.get('environment')))
    logger_info('config set: ' + environment)
    try:
        # connect to the postgres database
        conn = psycopg2.connect(dbname=ConfigClass.RDS_DBNAME, user=ConfigClass.RDS_USER,
                                password=ConfigClass.RDS_PWD, host=ConfigClass.RDS_HOST)
        cur = conn.cursor()
        table_name = ConfigClass.SQL_DB_NAME

        # get arguments
        dataset_geid = args['dataset_geid']
        refresh_token = args['refresh_token']
        access_token = args['access_token']

        logger_info('dataset_geid: {}, access_token: {}, refresh_token: {}'.format(
            dataset_geid, access_token, refresh_token))

        auth_token = {
            "at": access_token,
            "rt": refresh_token
        }

        locked_node = []
        files_locations = get_files_recursive(dataset_geid)
        # here add recursive read lock on the dataset
        locked_node, err = recursive_lock(dataset_geid)
        if err: raise err

        if len(files_locations) == 0:
            send_message(dataset_geid, "failed", 'no files in dataset')
            return
        download_from_minio(files_locations, auth_token)
        logger_info("files are downloaded from minio")

        getProcessOutput()
        result = read_result_file()

        logger_info('BIDS validation result: {}'.format(result))

        bids_output = json.loads(result)

        # remove bids folder after validate
        shutil.rmtree(TEMP_FOLDER)

        logger_info(f"Table name is: {table_name}")

        cur.execute(
            """
            SELECT *
            FROM {}.bids_results b
            """.format(table_name) +
            """
            WHERE b.dataset_geid = %s;
            """, [dataset_geid,]
        )
        record = cur.fetchone()

        current_time = datetime.utcfromtimestamp(time.time())

        logger_info(f"Validation time: {current_time}")
        # check whether the postgres database contains the record befor or not
        if not record:
            cur.execute(
                """
                INSERT INTO
                {}.bids_results(dataset_geid, created_time, updated_time, validate_output)
                """.format(table_name) + 
                """
                VALUES (%s, %s, %s, %s);
                """,
                [dataset_geid, current_time,
                    current_time, json.dumps(bids_output)]
            )
        else:
            cur.execute(
                """
                UPDATE {}.bids_results
                """.format(table_name) +
                """
                SET validate_output = %s, updated_time = %s
                WHERE dataset_geid = %s
                ;
                """,
                [json.dumps(bids_output), current_time, dataset_geid]
            )

        conn.commit()
        conn.close()

        send_message(dataset_geid, "success", bids_output)

    except Exception as e:
        logger_info(f"BIDs validator failed due to: {str(e)}")
        send_message(dataset_geid, "failed", str(e))
        # time.sleep(60*60)
        raise
    
    finally:
        for resource_key, operation in locked_node:
            unlock_resource(resource_key, operation)


if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Validate Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
