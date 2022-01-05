import argparse
import time
import traceback
from pathlib import Path
from typing import Any
from typing import Dict

import requests

from config import ConfigClass
from minio_client import Minio_Client_
from models import append_suffix_to_filepath
from models import get_timestamp
from neo4j_helper import Neo4jPathCheck
from services.approval.client import ApprovalServiceClient
from services.approval.models import CopyStatus
from utils import get_resource_by_geid
from utils import get_session_id
from utils import lock_resource
from utils import logger_info
from utils import unlock_resource
from utils import update_job


def main():
    environment = args.get('environment', 'test')
    logger_info('environment: ' + str(args.get('environment')))
    logger_info('config set: ' + environment)
    project_code = args['project_code']
    output_path = args['output_path']
    output_bucket = f'core-{project_code}'
    input_path = args['input_path']
    input_bucket = f'gr-{project_code}'
    operator = args['operator']
    job_id = args['job_id']
    session_id = get_session_id(job_id)
    request_id = args['request_id']
    minio_token = {
        'at': args['access_token'],
        'rt': args['refresh_token'],
    }
    copy_start_timestamp = get_timestamp()

    logger_info(f'Running file-copy with arguments: {args}')
    logger_info(f'Config environment: {ConfigClass.env}')
    logger_info(f'Using output bucket: {output_bucket}')
    logger_info(f'Using input bucket: {input_bucket}')
    logger_info(f'Starting to copy file: {input_path}')

    source_check = Neo4jPathCheck('Greenroom')
    input_node = source_check.get_file(project_code, f'{input_bucket}/{input_path}')
    if not input_node:
        raise ValueError(f'Input file "{input_path}" is not found in the database')

    destination_check = Neo4jPathCheck('VRECore')
    destination_folder = None

    approval_service_client = None
    approval_entity = None

    if request_id:
        approval_service_client = ApprovalServiceClient()
        approval_request = approval_service_client.get_approval_request(request_id)
        approval_entities = approval_service_client.get_approval_entities(request_id)
        approved_approval_entities = approval_entities.get_approved()

        try:
            approval_entity = approved_approval_entities[input_node.geid]
        except KeyError:
            raise ValueError(
                f'Input file "{input_path}" is not listed in approved entities for the request "{request_id}"'
            )

        try:
            approval_request_source = get_resource_by_geid(approval_request.source_geid)
            assert approval_request_source.is_archived is False
        except Exception:
            raise ValueError(f'Source folder from approval request "{approval_request}" does no longer exist')

        try:
            approval_request_destination = get_resource_by_geid(approval_request.destination_geid)
            assert approval_request_destination.is_archived is False
            approval_request_destination_path = approval_request_destination['display_path']
        except Exception:
            raise ValueError(f'Destination folder from approval request "{approval_request}" does no longer exist')

        approval_entity_path = approval_entities.get_path_until_top_parent(approval_entity)

        output_path_parts = [approval_request_destination_path]
        if approval_entity_path:
            output_path_parts.append(str(approval_entity_path))
        output_path_parts.append(approval_entity.name)

        output_path = '/'.join(output_path_parts)

        output_folder = str(Path(f'{output_bucket}/{output_path}').parent)
        lock_resource(output_folder, 'write')
        try:
            destination_folder = destination_check.create_path(
                project_code, approval_entity_path, approval_request_destination, operator
            )
        finally:
            unlock_resource(output_folder, 'write')

    destination_filepath = f'{output_bucket}/{output_path}'

    if not destination_folder:
        destination_folder = destination_check.get_folder(project_code, str(Path(output_path).parent))

    if not destination_folder:
        raise ValueError('Destination folder does no longer exist')

    if destination_folder.is_archived:
        raise ValueError('Destination folder already in trash bin')

    is_file_exists = destination_check.is_file_exists(project_code, destination_filepath)

    if is_file_exists:
        logger_info(f'File {output_path} already exists at destination')
        output_path = append_suffix_to_filepath(output_path, copy_start_timestamp)
        logger_info(f'Using new filename {output_path}')

    # lock the source as read lock, destination as write
    lock_resource("%s/%s" % (input_bucket, input_path), "read")
    lock_resource("%s/%s" % (output_bucket, output_path), "write")

    try:
        # copy minio object
        result = copy_object_single_file(output_bucket, output_path, input_bucket, input_path, minio_token)
        if is_file_exists or request_id:
            result['output_path'] = output_path
    finally:
        # unlock it after upload
        unlock_resource("%s/%s" % (input_bucket, input_path), "read")
        unlock_resource("%s/%s" % (output_bucket, output_path), "write")

    if approval_service_client and approval_entity:
        approval_service_client.update_copy_status(approval_entity, CopyStatus.COPIED)

    update_job(session_id, job_id, 'RUNNING', result)

    logger_info(f'Successfully copied file from {input_bucket}/{input_path} to {output_bucket}/{output_path}')


def debug_message_sender(message: str):
    url = ConfigClass.DATA_OPS_UT + "files/actions/message"
    response = requests.post(url, json={
        "message": message,
        "channel": "pipelinewatch"
    })
    if response.status_code != 200:
        print("code: " + str(response.status_code) + ": " + response.text)
    return


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
    parser.add_argument('-rid', '--request-id', help='Approval request id')
    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def copy_object_single_file(
    bucket, object_name: str, source_bucket, source_object_name: str, auth_token: Dict[str, Any]
) -> Dict[str, Any]:
    logger_info("[Copying source] {}::{}".format(source_bucket, source_object_name))
    logger_info("[Copying destination] {}::{}".format(bucket, object_name))
    try:
        # get size
        mc = Minio_Client_(auth_token["at"], auth_token["rt"])
        logger_info("========Minio_Client Initiated========")
        file_size_gb = mc.client.stat_object(source_bucket, source_object_name).size
        versioning = None
        if file_size_gb < 5e+9:
            logger_info("File size less than 5GiB")
            # move minio file objects
            # copy an object from a bucket to another.
            result = mc.copy_object(bucket, object_name, source_bucket, source_object_name)
            versioning = result.version_id
        else:
            logger_info("File size greater than 5GiB")
            temp_path = ConfigClass.TEMP_DIR + str(time.time())
            file_get = mc.client.fget_object(source_bucket, source_object_name, temp_path)
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


if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Copy Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
