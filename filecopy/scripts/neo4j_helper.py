import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

import requests
from requests import Response

from config import ConfigClass
from models import Node
from models import ResourceType
from services.approval.models import ApprovalEntity
from services.approval.models import ApprovalEntityPath
from utils import get_resource_by_geid


def get_children_nodes(start_geid):

    payload = {
        "label": "own",
        "start_label": "Folder",
        "start_params": {"global_entity_id":start_geid},
    }

    node_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
    response = requests.post(node_query_url, json=payload)
    ffs = [x.get("end_node") for x in response.json()]

    return ffs


def create_file_node(
    project,
    source_file,
    operator,
    parent_id,
    relative_path,
    minio_client,
    tags=None,
    attribute=None,
    new_name=None,
    extra_labels=None,
    extra_fields=None,
) -> Tuple[Node, Response, str]:
    if tags is None:
        tags = []

    if attribute is None:
        attribute = {}

    if extra_labels is None:
        extra_labels = ['VRECore']

    if extra_fields is None:
        extra_fields = {}

    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    file_name = new_name if new_name else source_file.get("name")
    # generate minio object path
    fuf_path = relative_path+"/"+file_name

    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    location = "minio://%s/%s/%s"%(minio_http, "core-"+project, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        "file_size": source_file.get("file_size", -1), # if the folder then it is -1
        "operator": operator,
        "uploader": source_file.get("uploader"),
        "name": file_name,
        "global_entity_id": geid,
        "location": location,
        "project_code": project,
        "tags": tags,
        "extra_labels": extra_labels,
        "display_path": fuf_path,
        "archived": False,
        "list_priority": 20,
        "generate_id": source_file.get("generate_id", None),
        **extra_fields
    }

    # adding the attribute set if exist
    manifest = source_file.get("manifest_id")
    if manifest:
        file_attribute.update({"manifest_id": manifest})
        file_attribute.update(attribute)

    new_file_node, new_relation = create_node_with_parent("File", file_attribute, parent_id)
    version_id = None
    # make minio copy
    try:
        # minio location is minio://http://<end_point>/bucket/user/object_path
        src_minio_path = source_file.get('location').split("//")[-1]
        _, src_bucket, src_obj_path = tuple(src_minio_path.split("/", 2))
        target_minio_path = location.split("//")[-1]
        _, target_bucket, target_obj_path = tuple(target_minio_path.split("/", 2))

        # here the minio api only accept the 5GB in copy. if >5GB we need to download
        # to local then reupload to target
        file_size_gb = minio_client.client.stat_object(src_bucket, src_obj_path).size
        if file_size_gb < 5e+9:
            print("File size less than 5GiB")
            # move minio file objects
            # copy an object from a bucket to another.
            result = minio_client.copy_object(target_bucket, target_obj_path, src_bucket, src_obj_path)
            version_id = result.version_id
        else:
            print("File size greater than 5GiB")
            temp_path = ConfigClass.TEMP_DIR + str(time.time())
            file_get = minio_client.client.fget_object(
                src_bucket, src_obj_path, temp_path)
            print("File fetched to local disk: {}".format(temp_path))
            result = minio_client.fput_object(target_bucket, target_obj_path, temp_path)
            version_id = result.version_id

        print("Minio Copy %s/%s Success"%(src_bucket, src_obj_path))
    except Exception as e:
        print("error when uploading: "+str(e))

    return new_file_node, new_relation, version_id


# some level of refactory needed here
# for us the deletion just archive the neo4j node
# but delete the minio object
def archived_file_node(
    project,
    source_file,
    operator,
    parent_id,
    relative_path,
    minio_client,
    tags=None,
    attribute=None,
    new_name=None,
    extra_labels=None,
    extra_fields=None,
) -> Tuple[Node, Response]:
    if tags is None:
        tags = []

    if attribute is None:
        attribute = {}

    if extra_labels is None:
        extra_labels = ['VRECore']

    if extra_fields is None:
        extra_fields = {}

    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    file_name = new_name if new_name else source_file.get("name")
    # generate minio object path
    fuf_path = relative_path+"/"+file_name

    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    location = "minio://%s/%s/%s"%(minio_http, "core-"+project, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        "file_size": source_file.get("file_size", -1), # if the folder then it is -1
        "operator": operator,
        "uploader": source_file.get("uploader"),
        "name": file_name,
        "global_entity_id": geid,
        "location": location,
        "project_code": project,
        "tags": tags,
        "extra_labels": extra_labels,
        "display_path": fuf_path,
        "archived": False,
        "list_priority": 20,
        **extra_fields
    }

    # adding the attribute set if exist
    manifest = source_file.get("manifest_id")
    if manifest:
        file_attribute.update({"manifest_id": manifest})
        file_attribute.update(attribute)

    new_file_node, new_relation = create_node_with_parent("File", file_attribute, parent_id)

    # make minio copy
    try:
        # mc = Minio_Client_(ConfigClass, access_token, refresh_token)
        # minio location is minio://http://<end_point>/bucket/user/object_path
        src_minio_path = source_file.get('location').split("//")[-1]
        _, src_bucket, src_obj_path = tuple(src_minio_path.split("/", 2))
        # target_minio_path = location.split("//")[-1]
        # _, target_bucket, target_obj_path = tuple(target_minio_path.split("/", 2))

        result = minio_client.client.remove_object(src_bucket, src_obj_path)
        print("Minio delete %s/%s Success"%(src_bucket, src_obj_path))
    except Exception as e:
        print("error when uploading: "+str(e))

    return new_file_node, new_relation


def create_folder_node(
    project_code,
    source_folder,
    operator,
    parent_node,
    relative_path,
    tags=None,
    new_name=None,
    extra_labels=None,
    extra_fields=None,
) -> Tuple[Node, Response]:
    if tags is None:
        tags = []

    if extra_labels is None:
        extra_labels = ['VRECore']

    if extra_fields is None:
        extra_fields = {}

    geid = requests.get(ConfigClass.COMMON_SERVICE + "utility/id").json().get("result")
    folder_name = source_folder.get("name")
    if new_name is not None:
        folder_name = new_name

    # then copy the node under the dataset
    folder_attribute = {
        "uploader": source_folder.get("uploader"),
        "operator": operator,
        "name": folder_name,
        "global_entity_id": geid,
        "folder_relative_path": relative_path,
        "display_path": relative_path + "/" + folder_name,
        "folder_level": parent_node.get("folder_level", -1) + 1,
        "project_code": project_code,
        "tags": tags,
        "archived": False,
        "extra_labels": extra_labels,
        "list_priority": 10,
        **extra_fields
    }
    folder_node, relation = create_node_with_parent("Folder", folder_attribute, parent_node.get('id'))

    return folder_node, relation


# this function will help to create a target node
# and connect to parent with "own" relationship
def create_node_with_parent(node_label, node_property, parent_id) -> Tuple[Node, Response]:
    # create the node with following attribute
    # - global_entity_id: unique identifier
    # - create_by: who import the files
    # - size: file size in byte (if it is a folder then size will be -1)
    # - create_time: neo4j timeobject (API will create but not passed in api)
    # - location: indicate the minio location as minio://http://<domain>/object
    create_node_url = ConfigClass.NEO4J_SERVICE + 'nodes/' + node_label
    response = requests.post(create_node_url, json=node_property)
    new_node = response.json()[0]

    # now create the relationship
    # the parent can be two possible: 1.dataset 2.folder under it
    create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
    new_relation = requests.post(create_node_url, json={"start_id": parent_id, "end_id": new_node.get("id")})

    return Node(new_node), new_relation


class Neo4jPathCheck:

    def __init__(self, zone: str) -> None:
        self.zone = zone
        self.minio_url = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT

    def _get_node(self, payload: Dict[str, Any]) -> Optional[Node]:
        url = f'{ConfigClass.NEO4J_SERVICE_V2}nodes/query'
        response = requests.post(url, json=payload, timeout=15)
        if response.status_code == 200:
            result = response.json()['result']
            if len(result) > 0:
                return Node(result[0])

        return None

    def is_file_exists(self, project_code: str, path: str) -> bool:
        """Check if file already exists at specified path."""

        node = self.get_file(project_code, path)

        return bool(node)

    def is_folder_exists(self, project_code: str, path: str) -> bool:
        """Check if folder already exists within project at specified path."""

        node = self.get_folder(project_code, path)

        return bool(node)

    def get_file(self, project_code: str, path: str) -> Optional[Node]:
        """Return file that exists within project at specified path or None."""

        location = f'minio://{self.minio_url}/{path}'

        payload = {
            'page': 0,
            'page_size': 1,
            'partial': False,
            'order_by': 'global_entity_id',
            'order_type': 'desc',
            'query': {
                'project_code': project_code,
                'location': location,
                'labels': [self.zone, ResourceType.FILE],
                'archived': False,
            }
        }

        return self._get_node(payload)

    def get_folder(self, project_code: str, path: Union[str, Path]) -> Optional[Node]:
        """Return folder that exists within project at specified path or None."""

        folder_path = Path(path)

        folder_relative_path = str(folder_path.parent)
        if folder_relative_path == '.':
            folder_relative_path = ''

        payload = {
            'page': 0,
            'page_size': 1,
            'partial': False,
            'order_by': 'global_entity_id',
            'order_type': 'desc',
            'query': {
                'project_code': project_code,
                'folder_relative_path': folder_relative_path,
                'name': folder_path.name,
                'labels': [self.zone, ResourceType.FOLDER],
                'archived': False,
            }
        }

        return self._get_node(payload)

    def create_path(
        self, project_code: str, approval_entity_path: ApprovalEntityPath, parent_folder_node: Node, operator: str
    ) -> Node:
        """Create all folders in a path (if they don't exist).

        Starting from parent folder node and return last folder node in a path."""

        path_to_create = ApprovalEntityPath(approval_entity_path)

        current_folder_path = Path(parent_folder_node['display_path'])
        while path_to_create:
            current_folder_entity: ApprovalEntity = path_to_create.pop(0)
            folder = self.get_folder(project_code, current_folder_path / current_folder_entity.name)
            if not folder:
                source_folder = get_resource_by_geid(current_folder_entity.entity_geid)
                folder, _ = create_folder_node(
                    project_code,
                    source_folder,
                    operator,
                    parent_folder_node,
                    str(current_folder_path),
                    source_folder['tags'],
                )

            current_folder_path /= current_folder_entity.name
            parent_folder_node = folder

        return parent_folder_node
