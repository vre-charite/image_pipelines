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

    node_query_url = ConfigClass.NEO4J_SERVICE_V1 + "relations/query"
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
        tags = source_file.get('tags', [])

    if attribute is None:
        attribute = {}

    if extra_labels is None:
        extra_labels = ['Greenroom']

    if extra_fields is None:
        extra_fields = {}

    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    file_name = new_name if new_name else source_file.get("name")
    # format minio object path
    fuf_path = relative_path+"/"+file_name

    minio_http = ("https://" if ConfigClass.MINIO_HTTPS else "http://") + ConfigClass.MINIO_ENDPOINT
    location = "minio://%s/%s/%s"%(minio_http, "gr-"+project, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        "file_size": new_file_object.size, # if the folder then it is -1
        "operator": operator,
        "uploader": source_file.get("uploader"),
        'dcm_id': source_file.get('dcm_id'),
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
        for attr in source_file:
            if "attr_" in attr:
                file_attribute.update({attr: source_file.get(attr)})

    new_file_node, new_relation = create_node_with_parent("File", file_attribute, parent_id)

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
    create_node_url = ConfigClass.NEO4J_SERVICE_V1 + 'nodes/' + node_label
    response = requests.post(create_node_url, json=node_property)
    new_node = response.json()[0]

    # now create the relationship
    # the parent can be two possible: 1.dataset 2.folder under it
    create_node_url = ConfigClass.NEO4J_SERVICE_V1 + 'relations/own'
    new_relation = create_relation(parent_id, new_node.get("id"))

    return new_node, new_relation


def create_relation(start_id:int, end_id:int, label="own") -> dict:
    '''
        function will create relationship between start node
        and end node specified by input in neo4j.
        It will return the new relation as dict
    '''

    create_node_url = ConfigClass.NEO4J_SERVICE_V1 + 'relations/%s'%(label)
    new_relation = requests.post(create_node_url, json={"start_id": start_id, "end_id": end_id})

    return new_relation