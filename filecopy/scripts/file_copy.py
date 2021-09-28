from config import config_singleton, set_config, config_factory
import os
import argparse
import time
import requests
import traceback
from minio_client import Minio_Client, Minio_Client_


def main():
    try:
        environment = args.get('environment', 'test')
        set_config(config_factory(environment))
        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        _config = config_singleton(environment)
        project_code = args['project_code']
        output_path = args['output_path']
        output_bucket = "core-" + project_code
        input_path = args['input_path']
        input_bucket = "gr-" + project_code
        operator = args['operator']
        job_id = args['job_id']
        # add new variable for the minio token
        token = {
            "at": args['access_token'],
            "rt": args['refresh_token']
        }

        logger_info('all varible: ' + str(args))
        logger_info('project_code: ' + project_code)
        logger_info('_config environment: ' + str(_config.env))
        logger_info('output_bucket: ' + output_bucket)
        logger_info('input_bucket: ' + input_bucket)
        logger_info('operator: ' + operator)
        logger_info('job_id: ' + job_id)

        # check if the input path is directory
        is_directory = False
        if is_directory:
            logger_info("Do not support copy as a folder")
            raise(Exception("[Invalid operation] input is a folder"))
        else:
            logger_info(f'starting to copy file: {input_path}')

        # copy minio object
        result = copy_object_single_file(
            output_bucket, output_path, input_bucket, input_path, token)
        
        update_job(job_id, 'RUNNING', result)

        logger_info(
            f'Successfully copied file from {input_path} to {output_path}')
    except Exception as e:
        raise


def debug_message_sender(message: str):
    _config = config_singleton()
    url = _config.DATA_OPS_UT + "files/actions/message"
    response = requests.post(url, json={
        "message": message,
        "channel": "pipelinewatch"
    })
    if response.status_code != 200:
        print("code: " + str(response.status_code) + ": " + response.text)
    return


def logger_info(message: str):
    debug_message_sender(message)
    print(message)


def parse_inputs():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-i', '--input-path', help='Sepecify input file',
                        metavar='Relative path, Object Name', required=True)
    parser.add_argument('-o', '--output-path', help='Sepecify output file',
                        metavar='Relative path, Object Name', required=True)
    parser.add_argument('-env', '--environment',
                        help='Environment', required=True)
    parser.add_argument('-p', '--project-code',
                        help='Project code', required=True)
    parser.add_argument('-op', '--operator',
                        help='Action operator', required=True)
    parser.add_argument('-j', '--job-id',
                        help='Job geid', required=True)

    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def recursively_get_paths(folder):
    paths = []
    for path, subdirs, files in os.walk(folder):
        for name in files:
            full_path = os.path.join(path, name)
            paths.append(full_path)
    return paths


def copy_object_single_file(bucket, object_name: str, source_bucket, source_object_name: str,
    auth_token: dict):
    logger_info("[Copying source] {}::{}".format(source_bucket, source_object_name))
    logger_info("[Copying destination] {}::{}".format(bucket, object_name))
    try:
        _config = config_singleton()
        # get size
        mc = Minio_Client_(_config, auth_token["at"], auth_token["rt"])
        logger_info("========Minio_Client Initiated========")
        file_size_gb = 0
        versioning = None
        if file_size_gb < 5:
            logger_info("File size less than 5GiB")
            # move minio file objects
            # copy an object from a bucket to another.
            result = mc.copy_object(
                bucket, object_name, source_bucket, source_object_name)
            versioning = result.version_id
        else:
            logger_info("File size greater than 5GiB")
            temp_path = _config.TEMP_DIR + str(time.time())
            file_get = mc.client.fget_object(
                source_bucket, source_object_name, temp_path)
            logger_info("File fetched to local disk: {}".format(temp_path))
            result = mc.fput_object(bucket, object_name, temp_path)
            versioning = result.version_id
            logger_info("File uploaded : {}".format(object_name))
        logger_info("Minio Object Copied")
        return {
            "versioning": versioning
        }
    except Exception as e:
        logger_info("[Fatal While Minio Copy] " + str(e))
        raise

def update_job(job_id, status, add_payload={}, progress=0):
    _config = config_singleton()
    url = _config.DATA_OPS_UT + "tasks"
    response = requests.put(url, json={
        "session_id": "*",
        "job_id": job_id,
        "status": status,
        "add_payload": add_payload,
        "progress": progress
    })
    logger_info(str(response.text))

if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Copy Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
