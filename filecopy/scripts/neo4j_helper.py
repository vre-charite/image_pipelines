import requests
import time
# from app.resources.error_handler import APIException
# from app.models.base_models import EAPIResponseCode

# from ..commons.logger_services.logger_factory_service import SrvLoggerFactory
from minio_client import Minio_Client_

from config import config_singleton

ConfigClass = config_singleton()

def create_relation(label, payload):
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/own", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j node API: {response.json()}", 
                status_code=response.status_code
            )
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j node API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return response.json()

def create_node(label, payload):
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + f"nodes/{label}", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j node API: {response.json()}", 
                status_code=response.status_code
            )
        created_node = response.json()[0]
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j node API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return created_node


def query_relation(relation_label, start_label, end_label, start_params={}, end_params={}):
    payload = {
        "label": relation_label,
        "start_label": start_label,
        "start_params": start_params,
        "end_label": end_label,
        "end_params": end_params,
    }
    try:
        response = requests.post(ConfigClass.NEO4J_SERVICE + "relations/query", json=payload)
        if response.status_code != 200:
            raise APIException(
                error_msg=f"Error calling neo4j relation query API: {response.json()}", 
                status_code=response.status_code
            )
    except Exception as e:
        raise APIException(
            error_msg=f"Error calling neo4j relation query API: {str(e)}", 
            status_code=EAPIResponseCode.internal_error.value
        )
    return response.json()

def get_node_by_geid(geid, label: str = None):
    try:
        response = None
        # since we have new api to directly fetch by label
        if label:
            payload = {
                'global_entity_id': geid,
            }
            node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/query"%(label)
            response = requests.post(node_query_url, json=payload)
        else:
            node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/geid/%s"%(geid)
            response = requests.get(node_query_url)

        # here if we dont find any node then return None
        if len(response.json()) == 0:
            return None

        return response.json()[0]
    except Exception as e:
        raise APIException(f"Error calling neo4j API: {str(e)}", EAPIResponseCode.internal_error.value)


def get_parent_node(current_node):
    # here we have to find the parent node and delete the relationship
    relation_query_url = ConfigClass.NEO4J_SERVICE + "relations/query"
    query_payload = {
        "label": "own",
        "end_label": current_node.get("labels")[0],
        "end_params": {"id":current_node.get("id")}
    }
    response = requests.post(relation_query_url, json=query_payload)
    # print(response.json()[0])
    parent_node_id = response.json()[0].get("start_node")

    return parent_node_id


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


def delete_relation_bw_nodes(start_id, end_id):
    # then delete the relationship between all the fils
    relation_delete_url = ConfigClass.NEO4J_SERVICE + "relations"
    delete_params = {
        "start_id": start_id,
        "end_id": end_id,
    }
    response = requests.delete(relation_delete_url, params=delete_params)
    return response


def delete_node(target_node, minio_client):

    node_label = target_node.get('labels')[0]
    node_id = target_node.get('id')
    node_delete_url = ConfigClass.NEO4J_SERVICE + "nodes/%s/node/%s"%(node_label, node_id)
    response = requests.delete(node_delete_url)

    # delete the file in minio if it is the file
    if node_label == "File":
        try:
            # mc = Minio_Client_(ConfigClass, access_token, refresh_token)

            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = target_node.get('location').split("//")[-1]
            _, bucket, obj_path = tuple(minio_path.split("/", 2))

            minio_client.delete_object(bucket, obj_path)
            print("Minio %s/%s Delete Success"%(bucket, obj_path))

        except Exception as e:
            print("error when deleting: "+str(e))


def create_file_node(project, source_file, operator, parent_id, relative_path, \
    minio_client, tags=[], attribute={}, new_name=None, \
    extra_labels=["VRECore"], extra_fields={}):

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
        target_minio_path = location.split("//")[-1]
        _, target_bucket, target_obj_path = tuple(target_minio_path.split("/", 2))

        file_size_gb = minio_client.client.stat_object(src_bucket, src_obj_path).size
        versioning = None
        if file_size_gb < 5e+9:
            print("File size less than 5GiB")
            # move minio file objects
            # copy an object from a bucket to another.
            result = minio_client.copy_object(target_bucket, target_obj_path, src_bucket, src_obj_path)
            versioning = result.version_id
        else:
            print("File size greater than 5GiB")
            temp_path = ConfigClass.TEMP_DIR + str(time.time())
            file_get = minio_client.client.fget_object(
                src_bucket, src_obj_path, temp_path)
            print("File fetched to local disk: {}".format(temp_path))
            result = minio_client.fput_object(target_bucket, target_obj_path, temp_path)
            versioning = result.version_id
            print("File uploaded : {}".format(target_obj_path))

        # minio_client.copy_object(target_bucket, target_obj_path, src_bucket, src_obj_path)
        print("Minio Copy %s/%s Success"%(src_bucket, src_obj_path))
    except Exception as e:
        print("error when uploading: "+str(e))

    return new_file_node, new_relation


# some level of refactory needed here
# for us the deletion just archive the neo4j node
# but delete the minio object
def archived_file_node(project, source_file, operator, parent_id, relative_path, \
    minio_client, tags=[], attribute={}, new_name=None, extra_labels=["VRECore"], \
    extra_fields={}):

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


def create_folder_node(dataset_code, source_folder, operator, parent_node, 
    relative_path, tags=[], new_name=None, extra_labels=["VRECore"], extra_fields={}):
    # fecth the geid from common service
    geid = requests.get(ConfigClass.COMMON_SERVICE+"utility/id").json().get("result")
    folder_name = new_name if new_name else source_folder.get("name")

    # then copy the node under the dataset
    folder_attribute = {
        "uploader": source_folder.get("uploader"),
        "operator": operator,
        "name": folder_name,
        "global_entity_id": geid,
        "folder_relative_path": relative_path,
        "display_path": relative_path+"/"+folder_name,
        "folder_level": parent_node.get("folder_level", -1)+1,
        "project_code": dataset_code,
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
def create_node_with_parent(node_label, node_property, parent_id):
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
    new_relation = requests.post(create_node_url, json={"start_id":parent_id, "end_id":new_node.get("id")})

    return new_node, new_relation
