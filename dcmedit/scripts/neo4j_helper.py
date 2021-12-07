import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple

import requests
from requests import Response

from config import ConfigClass

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
    new_file_object,
    tags=None,
    attribute=None,
    new_name=None,
    extra_labels=None,
    extra_fields=None,
) -> Tuple[dict, Response, str]:
    if tags is None:
        tags = []

    if attribute is None:
        attribute = {}

    if extra_labels is None:
        extra_labels = ['Greenroom']

    if extra_fields is None:
        extra_fields = {}

    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    file_name = new_name if new_name else source_file.get("name")
    # generate minio object path
    fuf_path = relative_path+"/"+file_name

    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    location = "minio://%s/%s/%s"%(minio_http, "gr-"+project, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        "file_size": new_file_object.size, # if the folder then it is -1
        "operator": operator,
        "uploader": source_file.get("uploader"),
        "generate_id": source_file.get("generate_id"),
        "name": file_name,
        "global_entity_id": geid,
        "location": location,
        "project_code": project,
        "tags": tags,
        "extra_labels": extra_labels,
        "display_path": fuf_path,
        "archived": False,
        "list_priority": 20,
        "version_id": new_file_object.version_id,
        **extra_fields
    }

    # adding the attribute set if exist
    manifest = source_file.get("manifest_id")
    if manifest:
        file_attribute.update({"manifest_id": manifest})
        file_attribute.update(attribute)

    new_file_node, new_relation = create_node_with_parent("File", file_attribute, parent_id)
    # version_id = None
    # # make minio copy
    # try:
    #     # minio location is minio://http://<end_point>/bucket/user/object_path
    #     src_minio_path = source_file.get('location').split("//")[-1]
    #     _, src_bucket, src_obj_path = tuple(src_minio_path.split("/", 2))
    #     target_minio_path = location.split("//")[-1]
    #     _, target_bucket, target_obj_path = tuple(target_minio_path.split("/", 2))

    #     # here the minio api only accept the 5GB in copy. if >5GB we need to download
    #     # to local then reupload to target
    #     file_size_gb = minio_client.client.stat_object(src_bucket, src_obj_path).size
    #     if file_size_gb < 5e+9:
    #         print("File size less than 5GiB")
    #         # move minio file objects
    #         # copy an object from a bucket to another.
    #         result = minio_client.copy_object(target_bucket, target_obj_path, src_bucket, src_obj_path)
    #         version_id = result.version_id
    #     else:
    #         print("File size greater than 5GiB")
    #         temp_path = ConfigClass.TEMP_DIR + str(time.time())
    #         file_get = minio_client.client.fget_object(
    #             src_bucket, src_obj_path, temp_path)
    #         print("File fetched to local disk: {}".format(temp_path))
    #         result = minio_client.fput_object(target_bucket, target_obj_path, temp_path)
    #         version_id = result.version_id

    #     print("Minio Copy %s/%s Success"%(src_bucket, src_obj_path))
    # except Exception as e:
    #     print("error when uploading: "+str(e))

    return new_file_node, new_relation


# this function will help to create a target node
# and connect to parent with "own" relationship
def create_node_with_parent(node_label, node_property, parent_id) -> Tuple[dict, Response]:
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
    new_relation = create_relation(parent_id, new_node.get("id"))

    return new_node, new_relation


def create_relation(start_id:int, end_id:int, label="own") -> dict:
    '''
        function will create relationship between start node
        and end node specified by input in neo4j.
        It will return the new relation as dict
    '''

    create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/%s'%(label)
    new_relation = requests.post(create_node_url, json={"start_id": start_id, "end_id": end_id})

    return new_relation