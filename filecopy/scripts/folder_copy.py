from config import config_singleton, set_config, config_factory
import os
import argparse
import time
import requests
import traceback
import datetime
from minio_client import Minio_Client, Minio_Client_
from utils import get_resource_bygeid, get_connected_nodes, location_decoder, http_query_node, \
    store_file_meta_data_v2, http_update_node, create_lineage_v3, \
    add_copied_with_approval, update_file_operation_status_v2, update_file_operation_logs, \
    get_resource_type, lock_resource, unlock_resource
from folder import FolderMgr
from copy import deepcopy

ConfigClass = None

def main():
    global ConfigClass
    try:
        environment = args.get('environment', 'test')
        set_config(config_factory(environment))
        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        _config = config_singleton(environment)
        ConfigClass = _config
        project_code = args['project_code']
        output_geid = args['output']
        input_geid = args['input']
        operator = args['operator']
        job_id = args['job_id']
        rename = args['rename']
        session_id = get_session_id(job_id)
        # add new variable for the minio token
        token = {
            "at": args['access_token'],
            "rt": args['refresh_token']
        }

        logger_info('all varible: ' + str(args))
        logger_info('project_code: ' + project_code)
        logger_info('_config environment: ' + str(_config.env))
        logger_info('output_geid: ' + output_geid)
        logger_info('input_geid: ' + input_geid)
        logger_info('operator: ' + operator)
        logger_info('job_id: ' + job_id)
        logger_info('rename: ' + rename)

        # do copy
        result = copy_execute(
            job_id, rename,
            output_geid, input_geid,
            project_code, operator,
            token,
        )
        
        update_job(session_id, job_id, 'SUCCEED')

        logger_info(
            f'Successfully copied folder from {input_geid} to {output_geid}')
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
    parser.add_argument('-i', '--input', help='Sepecify input geid',
                        metavar='Object Geid', required=True)
    parser.add_argument('-o', '--output', help='Sepecify output geid',
                        metavar='Object Geid', required=True)
    parser.add_argument('-env', '--environment',
                        help='Environment', required=True)
    parser.add_argument('-p', '--project-code',
                        help='Project code', required=True)
    parser.add_argument('-op', '--operator',
                        help='Action operator', required=True)
    parser.add_argument('-j', '--job-id',
                        help='Job geid', required=True)
    parser.add_argument('-r', '--rename',
                    help='rename', required=True)

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

def get_job(job_id):
    url = ConfigClass.DATA_OPS_UT + "tasks"
    task_response = requests.get(
        url,
        params={
            "session_id": "*",
            "job_id": job_id
        }
    )
    logger_info(url)
    logger_info(job_id)
    logger_info(task_response.text)
    my_task = task_response.json()['result'][0]
    return my_task

def get_session_id(job_id):
    job = get_job(job_id)
    return job["session_id"]

def copy_execute(job_id, new_name, dest_geid, input_geid, project_code, operator, auth_token: dict):
    source_folder_node = get_resource_bygeid(input_geid)
    dest_node = get_resource_bygeid(dest_geid)
    dest_node["resource_type"] = get_resource_type(dest_node["labels"])
    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    nodes_child = get_connected_nodes(
                source_folder_node['global_entity_id'], "output")
    uploader = source_folder_node.get("uploader", "admin")
    flattened_sources = []

    # here will ONLY unarchived file
    nodes_child_files = [
                node for node in nodes_child if "File" in node["labels"] and node.get("archived")==False]
    zone = 'vrecore'
    process_pipeline="data_transfer_folder"
    # generate meta information
    unix_process_time = datetime.datetime.utcnow().timestamp()
    # flatten resources
    minio_cli = Minio_Client_(ConfigClass, auth_token["at"], auth_token["rt"])
    for node in nodes_child_files:
        node['before_update'] = deepcopy(node)
        node['parent_folder'] = source_folder_node
        input_nodes = get_connected_nodes(
            node["global_entity_id"], "input")
        input_nodes = [
            node for node in input_nodes if 'Folder' in node['labels']]
        input_nodes.sort(key=lambda f: f['folder_level'])
        found_source_node = [
            node for node in input_nodes if node['global_entity_id'] == source_folder_node['global_entity_id']][0]
        path_relative_to_source_path = ''
        source_index = input_nodes.index(found_source_node)
        folder_name_list = [node['name']
                            for node in input_nodes[source_index + 1:]]
        path_relative_to_source_path = os.sep.join(folder_name_list)
        node['path_relative_to_source_path'] = path_relative_to_source_path
        node['ouput_relative_path'] = os.path.join(
            new_name, path_relative_to_source_path)
    flattened_sources += nodes_child_files
    logger_info('Resouce flattened: ' + str(len(nodes_child_files)))
    # copy resources
    for source in flattened_sources:
        source['resource_type'] = get_resource_type(source['labels'])
        location = source['location']
        ingestion_type, ingestion_host, ingestion_path = location_decoder(
            location)
        source['ingestion_type'] = ingestion_type
        source['ingestion_host'] = ingestion_host
        source['ingestion_path'] = ingestion_path
        ouput_relative_path = source.get('ouput_relative_path', '')
        input_path, output_path = get_output_payload(
            source, dest_node, ouput_relative_path=ouput_relative_path)
        source['input_path'] = input_path
        source['output_path'] = output_path
        output_bucket = "core-" + project_code
        input_bucket = "gr-" + project_code
        # do single file copy# copy minio object
        lock_resource(os.path.join(output_bucket, output_path))
        lock_resource(os.path.join(input_bucket, input_path))
        result = copy_object_single_file(
            output_bucket, output_path, input_bucket, input_path, minio_cli)
        # get task payloads
        versioning = result["versioning"]
        url = ConfigClass.DATA_OPS_UT + "tasks"
        task_response = requests.get(
            url,
            params={
                "session_id": "*",
                "job_id": job_id
            }
        )
        my_task = task_response.json()['result'][0]
        session_id = my_task['session_id']
        # Saving folder metadata
        created_folders_cache = []
        folder_mgr = FolderMgr(
                    created_folders_cache,
                    project_info["global_entity_id"],
                    project_code,
                    os.path.dirname(output_path),
                    [],
                    zone)
        folder_mgr.create(uploader)
        last_folder_node = folder_mgr.last_node
        # Saving file metadata
        # v2 API
        from_parents = {
            "global_entity_id": source['global_entity_id'],
            "original_geid": source['global_entity_id']
        }
        output_dir_path = os.path.dirname(output_path)
        file_node_stored = store_file_meta_data_v2(
            uploader,
            source["name"],
            output_dir_path,
            source.get('file_size', 0),
            'processed by data_transfer',
            zone,
            project_code,
            [tag for tag in source.get('tags', []) if tag != ConfigClass.copied_with_approval],
            source.get("generate_id", "undefined"),
            operator,
            from_parents=from_parents,
            process_pipeline=process_pipeline,
            parent_folder_geid=last_folder_node.global_entity_id,
            original_geid=source['global_entity_id'],
            bucket=output_bucket,
            object_path=output_path,
            version_id=versioning)
        copy_zippreview(
            source['global_entity_id'],
            file_node_stored['global_entity_id'])
        # update extra attibutes
        update_json = {}
        before_update = source['before_update']
        for k, v in before_update.items():
            if not k in file_node_stored:
                update_json[k] = v
        updated_file_node_stored = http_update_node(
            "File", file_node_stored['id'], update_json=update_json)
        logger_info("file_node_stored: " + str(file_node_stored['id']))
        logger_info("update_json:    " + str(update_json))
        logger_info("updated_file_node_stored:  " + updated_file_node_stored.text)
        # res = updated_file_node_stored.json()[0]
        # refresh_node(file_node_stored, res)
        logger_info('Saved meta v2')
        # create lineage
        create_lineage_v3(
            source['global_entity_id'],
            file_node_stored['global_entity_id'],
            project_code,
            process_pipeline,
            'data_transfer Processed',
            unix_process_time)
        logger_info('Created Lineage v3')
        res_update_audit_logs = update_file_operation_logs(
            uploader,
            operator,
            os.path.join('Greenroom', project_code, input_path),
            os.path.join('VRECore', project_code, output_path),
            source.get('file_size', 0),
            project_code,
            source.get("generate_id", "undefined")
        )
        logger_info('res_update_audit_logs: ' +
            str(res_update_audit_logs.status_code))
        unlock_resource(os.path.join(output_bucket, output_path))
        unlock_resource(os.path.join(input_bucket, input_path))
    logger_info("Loop done")
    res_source_folder_add_copied_with_approval, add_copied_with_approval_url, add_copied_with_approval_payload = \
        add_copied_with_approval(
            'Folder', source_folder_node['global_entity_id'], True)
    logger_info(res_source_folder_add_copied_with_approval.text)
    logger_info(add_copied_with_approval_url)
    logger_info(str(add_copied_with_approval_payload))
    logger_info("res_source_folder_add_copied_with_approval finished")



def copy_object_single_file(bucket, object_name: str, source_bucket, source_object_name: str,
    minio_cli: Minio_Client_):
    logger_info("[Copying source] {}::{}".format(source_bucket, source_object_name))
    logger_info("[Copying destination] {}::{}".format(bucket, object_name))
    try:
        _config = config_singleton()
        # get size
        mc = minio_cli
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

def update_job(session_id, job_id, status, add_payload={}, progress=0):
    _config = config_singleton()
    url = _config.DATA_OPS_UT + "tasks"
    response = requests.put(url, json={
        "session_id": session_id,
        "job_id": job_id,
        "status": status,
        "add_payload": add_payload,
        "progress": progress
    })
    logger_info(str(response.text))

def get_zone(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    zones = ['Greenroom', 'VRECore']
    for label in labels:
        if label in zones:
            return label
    return None

def get_output_payload(file_node, destination=None, ouput_relative_path=''):
    '''
    return inputpath, outputpath
    '''
    location = file_node['location']
    splits_loaction = location.split("://")
    ingestion_type = file_node['ingestion_type']
    ingestion_host = file_node['ingestion_host']
    ingestion_path = file_node['ingestion_path']
    if ingestion_type == "minio":
        splits_ingestion = ingestion_path.split("/", 1)
        source_bucket_name = splits_ingestion[0]
        source_object_name = splits_ingestion[1]
        path, source_name = os.path.split(source_object_name)
        if destination and destination['resource_type'] == 'Folder':
            path = os.path.join(
                destination['folder_relative_path'], destination['name'])
        copied_name = file_node['rename'] if file_node.get(
            'rename') else source_name
        output_path = os.path.join(path, ouput_relative_path, copied_name)
        root_folder = path.split('/')[0]
        if not destination:
            output_path = os.path.join(root_folder, ouput_relative_path, copied_name)
        return source_object_name, output_path

def copy_zippreview(old_geid, new_geid):
    url = ConfigClass.DATA_OPS_GR + "archive"
    get_params = {
        "file_geid": old_geid
    }
    response_get = requests.get(url=url, params=get_params)
    if response_get.status_code == 404:
        return
    if response_get.status_code != 200:
        raise Exception(response_get.text)
    json_response_get = response_get.json()
    archive_preview = json_response_get['result']
    post_url = ConfigClass.DATA_OPS_GR + "archive"
    post_json = {
        "file_geid": new_geid,
        "archive_preview": archive_preview
    }
    post_response = requests.post(url=post_url, json=post_json)
    if post_response.status_code != 200:
        raise Exception(post_response.text)

def refresh_node(target: dict, new: dict):
    for k, v in new.items():
        target[k] = v

if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Copy Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
