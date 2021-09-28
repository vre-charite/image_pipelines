import requests
import zipfile
import os
import subprocess
import json
import time
import psycopg2
import argparse
from datetime import datetime
from config import config_singleton, set_config, config_factory
import traceback
import shutil
from minio_client import Minio_Client, Minio_Client_

TEMP_FOLDER = './dataset/'


def get_dataset_url(geid, access_token, refresh_token):
    _config = config_singleton()
    DOWNLOAD_SERVICE = _config.DOWNLOAD_SERVICE

    dataset_url = "{}/v2/dataset/download/pre".format(DOWNLOAD_SERVICE)
    payload = {
        "dataset_geid": geid,
        "operator": "admin",
        "session_id": "image_pipeline"
    }
    headers = {
        'Authorization': "Bearer " + access_token,
        'Refresh-token': refresh_token,
        'Session-ID': 'image_pipeline'
    }
    res = requests.post(dataset_url, headers=headers, json=payload)
    data = res.json()

    return data['result']['payload']['hash_code']


def download_status(hash_code):
    _config = config_singleton()
    DOWNLOAD_SERVICE = _config.DOWNLOAD_SERVICE

    url = DOWNLOAD_SERVICE + f"/v1/download/status/{hash_code}"
    res = requests.get(url)
    res_json = res.json()
    if res_json.get('code') == 200:
        status = res_json.get('result').get('status')
        return status
    else:
        logger_info("Error when checking hash_code status")


def check_download_preparing_status(hash_code):
    while True:
        time.sleep(5)
        status = download_status(hash_code)
        logger_info("hash_code status: " + status)
        if status == 'READY_FOR_DOWNLOADING':
            break
    return status


def download_and_unzip(url: str, dest_folder: str):
    filename = "test.zip"
    file_path = os.path.join(dest_folder, filename)
    bids_folder = "dataset"

    r = requests.get(url, stream=True)
    if r.ok:
        logger_info("saving to", os.path.abspath(file_path))
        with open(file_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())

        os.mkdir(bids_folder)
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall(bids_folder)

    else:  # HTTP status code 4XX/5XX
        logger_info("Download failed: status code {}\n{}".format(
            r.status_code, r.text))


def debug_message_sender(message: str):
    _config = config_singleton()
    url = _config.DATA_OPS_UT + "files/actions/message"
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


def parse_inputs():
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


def send_message(dataset_geid, bids_output, is_error=False):
    _config = config_singleton()
    queue_url = _config.QUEUE_SERVICE + "broker/pub"
    post_json = {
        "event_type": "BIDS_VALIDATE_NOTIFICATION",
        "payload": {
            "status": "success",         # INIT/RUNNING/FINISH/ERROR
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

    if is_error:
        post_json = {
            "event_type": "BIDS_VALIDATE_NOTIFICATION",
            "payload": {
                "status": "failed",         # INIT/RUNNING/FINISH/ERROR
                "dataset": dataset_geid,
                "payload": None,
                "update_timestamp": time.time(),
                "error_msg": bids_output
            },
            "binary": True,
            "queue": "socketio",
            "routing_key": "socketio",
            "exchange": {
                "name": "socketio",
                "type": "fanout"
            }
        }

    queue_res = requests.post(queue_url, json=post_json)
    if queue_res.status_code != 200:
        logger_info("code: " + str(queue_res.status_code) +
                    ": " + queue_res.text)
    logger_info("sent message to queue")
    return


def get_files_recursive(dataset_geid, folder_geid=None, all_files=[]):
    _config = config_singleton()

    query = {
        "page": 0,
        "page_size": 10000,
        "order_by": "create_time",
        "order_type": "desc"
    }
    if folder_geid:
        query["folder_geid"] = folder_geid

    resp = requests.get(_config.DATASET_SERVICE +
                        "/dataset/{}/files".format(dataset_geid), params=query)
    for node in resp.json()["result"]['data']:
        if "File" in node["labels"]:
            all_files.append(node["location"])
        else:
            get_files_recursive(
                dataset_geid, node['global_entity_id'], all_files=all_files)
    return all_files


def download_from_minio(files_locations, auth_token):
    _config = config_singleton()
    mc = Minio_Client_(_config, auth_token["at"], auth_token["rt"])
    logger_info("========Minio_Client Initiated========")

    for file_location in files_locations:
        minio_path = file_location.split("//")[-1]
        _, bucket, obj_path = tuple(minio_path.split("/", 2))

        mc.client.fget_object(bucket, obj_path, TEMP_FOLDER + obj_path)
    logger_info("========Minio_Client download finished========")


def getProcessOutput():
    f = open("result.txt", "w")
    subprocess.run(
        ['bids-validator', TEMP_FOLDER + 'data', '--json'], universal_newlines=True, stdout=f)


def read_result_file():
    f = open("result.txt", "r")
    output = f.read()
    return output


def main():
    try:
        environment = args.get('environment', 'test')
        set_config(config_factory(environment))
        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        _config = config_singleton(environment)

        DOWNLOAD_SERVICE = _config.DOWNLOAD_SERVICE

        # connect to the postgres database
        conn = psycopg2.connect(dbname=_config.POSTGREL_DB, user=_config.POSTGREL_USER,
                                password=_config.POSTGREL_PWD, host=_config.POSTGREL_HOST)
        cur = conn.cursor()

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

        files_locations = get_files_recursive(dataset_geid)

        if len(files_locations) == 0:
            send_message(dataset_geid, 'no files in dataset', True)
            return
        download_from_minio(files_locations, auth_token)

        getProcessOutput()
        result = read_result_file()

        logger_info('BIDS validation result: {}'.format(result))

        try:
            bids_output = json.loads(result)
        except Exception as e:
            logger_info(str(e))
            send_message(dataset_geid, str(e), True)
            time.sleep(60*60)
            return

        # remove bids folder after validate
        shutil.rmtree(TEMP_FOLDER)

        cur.execute(
            """
            SELECT * 
            FROM indoc_vre.bids_results b 
            WHERE b.dataset_geid = %s;
            """,
            [dataset_geid, ]
        )
        record = cur.fetchone()

        current_time = datetime.utcfromtimestamp(time.time())
        if not record:
            cur.execute(
                """
                INSERT INTO
                indoc_vre.bids_results(dataset_geid, created_time, updated_time, validate_output)
                VALUES (%s, %s, %s, %s);
                """,
                [dataset_geid, current_time,
                    current_time, json.dumps(bids_output)]
            )
        else:
            cur.execute(
                """
                UPDATE indoc_vre.bids_results
                SET validate_output = %s, updated_time = %s
                WHERE dataset_geid = %s
                ;
                """,
                [json.dumps(bids_output), current_time, dataset_geid]
            )

        conn.commit()
        conn.close()

        send_message(dataset_geid, bids_output)

    except Exception as e:
        raise


if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Validate Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
