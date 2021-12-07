import argparse
import os
import traceback
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import requests

from config import ConfigClass
from minio_client import Minio_Client_
from models import append_suffix_to_filepath
from models import get_timestamp
from models import Node
from models import NodeList
from models import ResourceType
from neo4j_helper import create_file_node
from neo4j_helper import create_folder_node
from neo4j_helper import get_children_nodes
from neo4j_helper import Neo4jPathCheck
from services.approval.client import ApprovalEntityClient
from services.approval.models import ApprovedEntities
from services.approval.models import CopyStatus
from utils import get_resource_by_geid
from utils import get_session_id
from utils import http_query_node
from utils import http_update_node
from utils import lock_resource
from utils import logger_info
from utils import MetaDataFactory
from utils import unlock_resource
from utils import update_job


PROCESS_PIPELINE = "data_transfer_folder"
PIPELINE_DESC = '''
    the script will copy the folder from greenroom to core recursively
'''
OPERATION_TYPE = "data_transfer"


class DuplicatedFileNames:
    """Store information about source files that already exist at destination.

    File path should be used without the bucket name.
    Use: 'admin/folder/file.txt'
    Do not use: 'gr-project-code/admin/folder/file.txt'
    """

    def __init__(self) -> None:
        self.filename_timestamp = get_timestamp()
        self.files = {}

    def add(self, filepath: str) -> str:
        """Store information about duplicated file and return new filename that should be used."""

        path = Path(filepath)

        self.files[path] = append_suffix_to_filepath(path.name, self.filename_timestamp)

        return self.files[path]

    def get(self, filepath: str, default: Optional[str] = '') -> str:
        """Return filename by filepath if it exists or return default value."""

        path = Path(filepath)

        try:
            return self.files[path]
        except KeyError:
            pass

        return default


# TODO might be use adaptor to group the recursive?
class CopyObjects:
    def __init__(
        self,
        minio_client: Minio_Client_,
        metadata_factory: MetaDataFactory,
        duplicated_files: Optional[DuplicatedFileNames] = None,
        destination_check: Optional[Neo4jPathCheck] = None,
        approved_entities: Optional[ApprovedEntities] = None,
        approval_entity_client: Optional[ApprovalEntityClient] = None,
    ):
        self.mc = minio_client
        self.metadata_factory = metadata_factory

        if duplicated_files is None:
            duplicated_files = DuplicatedFileNames()
        self.duplicated_files = duplicated_files

        if destination_check is None:
            destination_check = Neo4jPathCheck('VRECore')
        self.destination_check = destination_check

        self.approved_entities = approved_entities
        self.approval_entity_client = approval_entity_client

        self.project = self.metadata_factory.project
        self.oper = self.metadata_factory.oper

    def recursive_copy(self, current_nodes: NodeList, current_root_path: str, parent_node: Node) -> None:
        """Copy the files under the project neo4j node to dataset node."""

        for node in current_nodes:
            self.copy_one_node(node, current_root_path, parent_node)

    def generate_file_metadata(self, node: Node, new_node: Node, new_node_version_id: str):
        source_geid = node.get("global_entity_id")
        target_geid = new_node.get("global_entity_id")
        # project_code = self.project.get("code")
        # also transfer the saved preview info to copied one
        copy_zippreview(source_geid, target_geid)

        # create the new node in atlas for lineage linking
        guid = self.metadata_factory.create_catalog_entity(new_node)

        # create the lineage link between greenroom -> relation -> core
        self.metadata_factory.create_lineage_v3(source_geid, target_geid)

        # create the elastic search index for advance search
        self.metadata_factory.create_es_search_index(new_node, node, "File", guid)

        # create the file stream/operational logs index in elastic search
        res_update_audit_logs = self.metadata_factory.update_file_operation_logs(
            os.path.join('Greenroom', node.get("display_path", "")),
            os.path.join('VRECore', new_node.get("display_path", "")),
        )
        logger_info('res_update_audit_logs: ' + str(res_update_audit_logs.status_code))

        # update old node to have system tag
        update_json = {"system_tags": ["copied-to-core"], "guid": guid, "version_id": new_node_version_id}
        http_update_node("File", node.get("id"), update_json)

    def is_node_approved(self, node: Node) -> bool:
        """Check if node geid is in a list of approved entities.

        If approved entities are not set then node is considered approved.
        """

        if self.approved_entities is None:
            return True

        return node.geid in self.approved_entities

    def update_approval_entity_copy_status_for_node(self, node: Node, copy_status: CopyStatus) -> None:
        """Update copy status field for approval entity related to node."""

        if not self.approval_entity_client:
            return

        if not self.approved_entities:
            return

        approval_entity = self.approved_entities[node.geid]

        self.approval_entity_client.update_copy_status(approval_entity, copy_status)

    def copy_one_node(self, node: Node, current_root_path: str, parent_node: Node) -> None:
        # update here if the folder/file is archived then skip
        if node.get("archived", False):
            return

        node_geid = node.geid

        if node.is_file:
            if not self.is_node_approved(node):
                return

            # TODO simplify here
            minio_path = node.get('location').split("//")[-1]
            _, bucket, old_path = tuple(minio_path.split("/", 2))

            destination_filename = self.duplicated_files.get(old_path, node.name)

            # file will need extra step to get all attribute
            # the format of attribute is {"attr_<field>": "value"}
            attr = {x: node[x] for x in node if "attr" in x}
            tags = node.get("tags")
            extra = {
                "system_tags": ["copied-to-core"], 
                "parent_folder_geid": parent_node.get("global_entity_id")
            }

            # create the copied node
            new_node, _, version_id = create_file_node(
                self.project.get("code"),
                node,
                self.oper,
                parent_node.get('id'),
                current_root_path,
                self.mc,
                tags=tags,
                attribute=attr,
                new_name=destination_filename,
                extra_fields=extra,
            )

            self.generate_file_metadata(node, new_node, version_id)

            self.update_approval_entity_copy_status_for_node(node, CopyStatus.COPIED)

        # else it is folder will trigger the recursive
        elif node.is_folder:

            project_code = self.project.get("code")
            folder_path = f'{current_root_path}/{node.name}'
            existing_folder = self.destination_check.get_folder(project_code, folder_path)
            if existing_folder:
                new_node = existing_folder
            else:
                # first create the folder
                tags = node.get("tags")
                extra = {
                    "system_tags": ["copied-to-core"], 
                    # "parent_folder_geid": parent_node.get("global_entity_id")
                }
                
                new_node, _ = create_folder_node(
                    self.project.get("code"),
                    node,
                    self.oper,
                    parent_node,
                    current_root_path,
                    tags=tags,
                    extra_fields=extra,
                )

                # update old node to have system tag
                # TODO remove this? since we add it above
                update_json = {"system_tags": ["copied-to-core"]}
                http_update_node(ResourceType.FOLDER, node.get("id"), update_json)

                # metadata creation
                self.metadata_factory.create_es_search_index(new_node, node, ResourceType.FOLDER, "")

            # seconds recursively go throught the folder/subfolder by same proccess
            children_nodes = get_children_nodes(node_geid)
            children_nodes = NodeList(children_nodes)
            self.recursive_copy(children_nodes, folder_path, new_node)

        return


def recursive_lock(
    locked_nodes: List[Tuple[str, str]],
    project_code: str,
    nodes: NodeList,
    destination_path: str,
    approved_entities: Optional[ApprovedEntities],
) -> DuplicatedFileNames:
    """The function will recursively lock the node tree."""

    # locked_nodes here is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.

    duplicated_files = DuplicatedFileNames()
    destination_check = Neo4jPathCheck('VRECore')

    # TODO lock

    def recur_walker(current_nodes: NodeList, current_destination_path: str) -> None:
        """Recursively trace down the node tree and run the lock function on folders."""

        for node in current_nodes:
            # update here if the folder/file is archived then skip
            if node.get("archived", False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source as read operation,
            # and the target will be write operation
            if node['display_path'] != node['uploader']:
                if node.is_file:
                    if approved_entities and node.geid not in approved_entities:
                        continue

                source_key = f'gr-{project_code}/{node["display_path"]}'
                lock_resource(source_key, "read")
                locked_nodes.append((source_key, "read"))

                output_bucket = f'core-{project_code}/{current_destination_path}'

                if node.is_file:
                    destination_filepath = f'{output_bucket}/{node.name}'
                    if destination_check.is_file_exists(project_code, destination_filepath):
                        logger_info(f'File {destination_filepath} already exists at destination')
                        node['name'] = duplicated_files.add(node['display_path'])
                        logger_info(f'Using new filename {node.name}')

                target_key = f'{output_bucket}/{node.name}'
                lock_resource(target_key, "write")
                locked_nodes.append((target_key, "write"))

            # open the next recursive loop if it is folder
            if node.is_folder:
                next_root = f'{current_destination_path}/{node.name}'
                children_nodes = NodeList(get_children_nodes(node.geid))
                recur_walker(children_nodes, next_root)

    recur_walker(nodes, destination_path)

    return duplicated_files


def copy_execute(
    dest_geid: str,
    input_geid: str,
    project_code: str,
    operator: str,
    request_id: Optional[str],
    auth_token: Dict[str, Any],
) -> None:
    source_node = get_resource_by_geid(input_geid)
    dest_node = get_resource_by_geid(dest_geid)

    if dest_node.is_archived:
        raise ValueError('Destination is already in trash bin')

    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]

    print(" - source node:", source_node)
    print()
    print(" - target node:", dest_node)
    print()
    print(" - project node:", project_info)
    print("======")

    approval_entity_client = None
    approved_entities = None

    if request_id:
        approval_entity_client = ApprovalEntityClient()
        approved_entities = approval_entity_client.get_approved_entities(request_id)

    locked_nodes = []
    try:
        source_nodes = NodeList([source_node])
        destination_path = dest_node.get('display_path')

        duplicated_files = recursive_lock(
            locked_nodes, project_info.get('code'), source_nodes, destination_path, approved_entities
        )

        # initialize the minio outside to keep one instance of credential
        # if dont do so, the large folder will cause the initial token expire
        mc = Minio_Client_(auth_token['at'], auth_token['rt'])

        target_zone = 'VRECore'
        metadata_factory = MetaDataFactory(
            project_info, operator, target_zone, PROCESS_PIPELINE, PIPELINE_DESC, OPERATION_TYPE
        )

        copy_object = CopyObjects(
            mc,
            metadata_factory,
            duplicated_files,
            Neo4jPathCheck(target_zone),
            approved_entities,
            approval_entity_client,
        )
        copy_object.recursive_copy(source_nodes, destination_path, Node(dest_node))
    finally:
        # here we unlock the locked nodes ONLY
        print("Start to unlock the nodes")
        for resource_key, operation in locked_nodes:
            unlock_resource(resource_key, operation)


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
    parser.add_argument('-rid', '--request-id', help='Approval request id')
    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def main():

    job_id = args['job_id']
    session_id = get_session_id(job_id)

    try:
        environment = args.get('environment', 'test')
        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        project_code = args['project_code']
        output_geid = args['output']
        input_geid = args['input']
        operator = args['operator']
        request_id = args['request_id']
        minio_token = {
            'at': args['access_token'],
            'rt': args['refresh_token'],
        }

        logger_info(f'Running folder-copy with arguments: {args}')
        logger_info(f'Config environment: {ConfigClass.env}')
        logger_info(f'Using output geid: {output_geid}')
        logger_info(f'Using input geid: {input_geid}')

        copy_execute(output_geid, input_geid, project_code, operator, request_id, minio_token)

        update_job(session_id, job_id, 'SUCCEED')

        logger_info(f'Successfully copied folder from {input_geid} to {output_geid}')
    except Exception as e:
        print("ERROR:", str(e))
        update_job(session_id, job_id, 'TERMINATED')
        raise e


if __name__ == "__main__":
    try:
        args = parse_inputs()
        main()
    except Exception as e:
        logger_info("[Copy Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise
