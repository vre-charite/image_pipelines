from config import config_singleton, set_config, config_factory
import os 
import argparse
import time
import requests
import traceback
import re
from minio_client import Minio_Client, Minio_Client_

def parse_inputs():
    parser = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-i', '--input-path', help='Sepecify input file', 
        metavar='FILE/Folder', required=True)
    parser.add_argument('-o', '--output-path', help='Sepecify output file', 
        metavar='PATH', required=True)
    parser.add_argument('-t', '--trash-path', help='Trash folder path', 
        metavar='PATH', required=True)
    parser.add_argument('-env', '--environment',
                        help='Environment', required=True)
    parser.add_argument('-p', '--project-code',
                        help='Project code', required=True)
    parser.add_argument('-op', '--operator',
                        help='Action operator', required=True)

    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True)

    arguments = vars(parser.parse_args())
    return arguments

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


def delete_object_single_file(source_bucket, source_object_name: str, auth_token:dict, version=None):
    logger_info("[Deleting source] {}::{}".format(source_bucket, source_object_name))
    try:
        _config = config_singleton()
        # get size
        mc = Minio_Client_(_config, auth_token["at"], auth_token["rt"])
        logger_info("========Minio_Client Initiated========")
        # move minio file objects
        # copy an object from a bucket to another.
        result = mc.client.remove_object(source_bucket, source_object_name)
        logger_info("Minio Object Deleted")
    except Exception as e:
        logger_info("[Fatal While Minio Copy] " + str(e))
        raise

def location_decoder(location: str):
    '''
    decode resource location
    return ingestion_type, ingestion_host, ingestion_path
    '''
    splits_loaction = location.split("://", 1)
    ingestion_type = splits_loaction[0]
    ingestion_url = splits_loaction[1]
    path_splits =  re.split(r"(?<!/)/(?!/)", ingestion_url, 1)
    ingestion_host = path_splits[0]
    ingestion_path = path_splits[1]
    return ingestion_type, ingestion_host, ingestion_path

def main():
    try:
        environment = args.get('environment', 'test')
        set_config(config_factory(environment))
        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        _config = config_singleton(environment)
        logger_info('_config environment: ' + str(_config.env))
        output_file = args['output_path']
        output_path = os.path.dirname(output_file)
        logger_info(f'file path is: {output_path}')
        input_path = args['input_path']
        trash_path = args['trash_path']
        # add new variable for the minio token
        token = {
            "at": args['access_token'],
            "rt": args['refresh_token']
        }
        
        ingestion_type, ingestion_host, ingestion_path = location_decoder(
            input_path)
        splits_ingestion = ingestion_path.split("/", 1)
        source_bucket_name = splits_ingestion[0]
        source_object_name = splits_ingestion[1]
        delete_object_single_file(source_bucket_name, source_object_name, token)
        logger_info(f'Successfully moved file from {input_path} to {output_file}')
    except Exception as e:
        logger_info(f'Failed to move file from {input_path} to {output_file}\n {e}')
        raise

if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Delete Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise

