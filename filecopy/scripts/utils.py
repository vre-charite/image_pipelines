from config import config_singleton
import requests
import re
import os

def fetch_geid(id_type=""):
    ConfigClass = config_singleton()
    ## fetch global entity id
    entity_id_url = ConfigClass.UTILITY_SERVICE + "/v1/utility/id?entity_type={}".format(id_type)
    respon_entity_id_fetched = requests.get(entity_id_url)
    if respon_entity_id_fetched.status_code == 200:
        pass
    else:
        raise Exception('Entity id fetch failed: ' + entity_id_url + ": " + str(respon_entity_id_fetched.text))
    trash_geid = respon_entity_id_fetched.json()['result']
    return trash_geid

def http_query_node(primary_label, query_params={}):
    '''
    primary_label i.e. Folder, File, Container
    '''
    ConfigClass = config_singleton()
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/{}/query".format(primary_label)
    response = requests.post(node_query_url, json=payload)
    return response

def get_resource_bygeid(geid):
    '''
    if not exist return None
    '''
    ConfigClass = config_singleton()
    url = ConfigClass.NEO4J_SERVICE_V2 + "nodes/query"
    payload_file = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['File']
        }
    }
    payload_folder = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Folder']
        }
    }
    payload_project = {
        "page": 0,
        "page_size": 1,
        "partial": False,
        "order_by": "global_entity_id",
        "order_type": "desc",
        "query": {
            "global_entity_id": geid,
            "labels": ['Container']
        }
    }
    response_file = requests.post(url, json=payload_file)
    if response_file.status_code == 200:
        result = response_file.json()['result']
        if len(result) > 0:
            return result[0]
    response_folder = requests.post(url, json=payload_folder)
    if response_folder.status_code == 200:
        result = response_folder.json()['result']
        if len(result) > 0:
            return result[0]
    response_project = requests.post(url, json=payload_project)
    if response_project.status_code == 200:
        result = response_project.json()['result']
        if len(result) > 0:
            return result[0]
    raise Exception('Not found resource: ' + geid)

def get_resource_type(labels: list):
    '''
    Get resource type by neo4j labels
    '''
    resources = ['File', 'TrashFile', 'Folder', 'Container']
    for label in labels:
        if label in resources:
            return label
    return None

def get_connected_nodes(geid, direction: str = "both"):
    '''
    return a list of nodes
    '''
    ConfigClass = config_singleton()
    if direction == 'both':
        params = {
            "direction": "input"
        }
        url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        connected_nodes = response.json()['result']
        params = {
            "direction": "output"
        }
        url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise Exception('Internal error for neo4j service, \
                when get_connected, geid: ' + str(geid))
        return connected_nodes + response.json()['result']
    params = {
        "direction": direction
    }
    url = ConfigClass.NEO4J_SERVICE + "relations/connected/{}".format(geid)
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception('Internal error for neo4j service, \
            when get_connected, geid: ' + str(geid))
    connected_nodes = response.json()['result']
    return connected_nodes


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


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    ConfigClass = config_singleton()
    update_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/node/{}".format(primary_label, neo4j_id)
    res = requests.put(url=update_url, json=update_json)
    return res


def create_lineage_v3(input_geid, output_geid, projectCode, pipelineName, description, create_time):
    '''
    create lineage
    payload = {
        "input_geid": "string",
        "output_geid": "string",
        "project_code": "string",
        "pipeline_name": "string",
        "description": "string"
    }
    '''
    ConfigClass = config_singleton()
    my_url = ConfigClass.PROVENANCE_SERVICE
    payload = {
        "input_geid": input_geid,
        "output_geid": output_geid,
        "project_code": projectCode,
        "pipeline_name": pipelineName,
        "description": description
    }
    res = requests.post(
            url=my_url + 'lineage',
            json=payload
    )
    if res.status_code == 200:
        return res.json()
    else:
        raise(Exception(res.text))

def add_copied_with_approval(resource_type, geid, inherit=False):
    # only can be used to transfer data to VRE CORE
    # neo4j version -----------------------------------------------------------------------------------
    ConfigClass = config_singleton()
    url = ConfigClass.DATA_OPS_UT_V2 + "{}/{}/systags".format(resource_type, geid)
    request_payload = {
        "systags": [
            ConfigClass.copied_with_approval
        ],
        "inherit": inherit
    }
    response = requests.post(url, json=request_payload)
    return response, url, request_payload

def get_frontend_zone(my_disk_namespace: str):
    '''
    disk namespace to path
    '''
    return {
        "greenroom": "Green Room",
        "vre": "VRE Core",
        "vrecore": "VRE Core"
    }.get(my_disk_namespace, None)

def update_file_operation_status_v2(session_id, job_id, zone, status, payload={}):
    '''
    Endpoint
    ConfigClass.data_ops_util_host/v1/tasks
    '''
    ConfigClass = config_singleton()
    url = ConfigClass.DATA_OPS_UT + 'tasks'
    payload = {
        "session_id": session_id,
        "job_id": job_id,
        "status": status,
        "progress": "100",
        "add_payload": {
            "zone": zone,
            "frontend_zone": get_frontend_zone(zone),
            **payload
        }
    }
    res_update_status = requests.put(
        url,
        json=payload
    )
    return res_update_status


def update_file_operation_logs(owner, operator, input_file_path, output_file_path,
                               file_size, project_code, generate_id, operation_type="data_transfer", extra=None):
    '''
    Endpoint
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    '''
    # new audit log api
    ConfigClass = config_singleton()
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    payload_audit_log = {
        "action": operation_type,
        "operator": operator,
        "target": input_file_path,
        "outcome": output_file_path,
        "resource": "file",
        "display_name": os.path.basename(input_file_path),
        "project_code": project_code,
        "extra": extra if extra else {}
    }
    res_audit_logs = requests.post(
        url_audit_log,
        json=payload_audit_log
    )
    return res_audit_logs

def store_file_meta_data_v2(uploader, output_file_name, output_path, file_size, desc, namespace,
                            project_name, labels, generate_id="undefined", operator=None,
                            from_parents=None, process_pipeline=None, parent_folder_geid=None, original_geid=None,
                            bucket="", object_path="", version_id=""):
    file_data_mgr = SrvFileDataMgr()
    return file_data_mgr.create(
        uploader,
        output_file_name,
        output_path,
        file_size,
        desc,
        namespace,
        project_name,
        labels,
        generate_id,
        operator,
        from_parents,
        process_pipeline,
        parent_folder_geid=parent_folder_geid,
        original_geid=original_geid,
        bucket=bucket,
        object_path=object_path,
        version_id=version_id)

def lock_resource(resource_key):
    ConfigClass = config_singleton()
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.post(url, json=post_json)
    return response


def unlock_resource(resource_key):
    ConfigClass = config_singleton()
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.delete(url, json=post_json)
    return response

class SrvFileDataMgr():
    ConfigClass = config_singleton()
    base_url = ConfigClass.DATA_OPS_UT

    def __init__(self):
        self.name = "file_data_mgr"

    def create(self, uploader, file_name, path, file_size, desc, namespace,
               project_code, labels, generate_id, operator=None,
               from_parents=None, process_pipeline=None, parent_folder_geid=None, original_geid=None,
               bucket="", object_path="", version_id=""):

        # fetch geid
        global_entity_id = self.fetch_guid()

        url = self.base_url + "filedata"
        post_json_form = {
            "global_entity_id": global_entity_id,
            "uploader": uploader,
            "file_name": file_name,
            "path": path,
            "file_size": file_size,
            "description": desc,
            "namespace": namespace,
            "project_code": project_code,
            "labels": labels,
            "generate_id": generate_id,
            "parent_folder_geid": parent_folder_geid if parent_folder_geid else "",
            "original_geid": original_geid,
            "bucket": bucket,
            "minio_object_path": object_path,
            "version_id": version_id
        }
        if operator:
            post_json_form['operator'] = operator
        if from_parents:
            post_json_form['parent_query'] = from_parents
        if process_pipeline:
            post_json_form['process_pipeline'] = process_pipeline
        res = requests.post(url=url, json=post_json_form)
        if res.status_code == 200:
            return res.json()['result']
        else:
            raise Exception(str({
                "error": "SrvFileDataMgr create meta failed",
                "errorcode": res.status_code,
                "payload": post_json_form,
                "error_msg": res.text
            }))

    def archive(self, path, file_name, trash_path, trash_file_name, operator, file_name_suffix, trash_geid, _logger, updated_original_file_path=None):
        ConfigClass = config_singleton()
        url = ConfigClass.CATALOGUING_SERVICE_V2 + "filedata"
        dele_json_form = {
            "path": path,
            "file_name": file_name,
            "trash_path": trash_path,
            "trash_file_name": trash_file_name,
            "operator": operator,
            "file_name_suffix": file_name_suffix,
            "trash_geid": trash_geid,
            "updated_original_file_path": updated_original_file_path
        }
        res = requests.delete(url=url, json=dele_json_form)
        _logger.debug(
            "dele_json_form CATALOGUING_SERVICE_V2 payload" + str(dele_json_form))
        _logger.debug(
            "dele_json_form CATALOGUING_SERVICE_V2 result" + res.text)
        if res.status_code == 200:
            return res.json()
        else:
            return {
                "error": "archive meta failed: " + url,
                "errorcode": res.status_code,
                "error_msg": res.text,
                "payload": dele_json_form
            }

    def archive_in_neo4j(self, path, file_name, project_code, updated_file_name):
        ConfigClass = config_singleton()
        try:
            parent_full_path = path + "/" + file_name
            parent_query = {
                "full_path": parent_full_path,
            }
            res_parent_gotten = http_query_node("File", parent_query)
            print("res_parent_gotten", res_parent_gotten.text)
            if res_parent_gotten.status_code == 200:
                print('updated res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "archive meta in neo4j failed when getting parent",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query),
                    "url": res_parent_gotten.url
                }
            parent_node = res_parent_gotten.json()[0]
            neo4j_id = parent_node['id']
            # update parent file
            update_url = ConfigClass.NEO4J_SERVICE + \
                "nodes/File/node/{}".format(neo4j_id)
            update_json = {
                "archived": True,
                "name": updated_file_name,
                "full_path": path + "/" + updated_file_name
            }
            res = requests.put(url=update_url, json=update_json)
            if res.status_code == 200:
                print('updated archive_in_neo4j: ', res.json())
            else:
                return {
                    "error": "archive meta in neo4j failed",
                    "errorcode": res.status_code,
                    "error_msg": res.text + "-----------" + update_url,
                    "payload": str(update_json)
                }
        except Exception as e:
            return {
                "error": "archive meta in neo4j failed",
                "errorcode": 500,
                "error_msg": str(e),
                "detail": "archive_in_neo4j code error"
            }

    def add_approval_copy_for_neo4j(self, geid, project_code):
        ConfigClass = config_singleton()
        try:
            # get project information
            get_project_url = ConfigClass.NEO4J_SERVICE + "nodes/Container/query"
            get_project_json = {
                "code": project_code
            }
            res_project_gotten = requests.post(
                url=get_project_url, json=get_project_json)
            if res_project_gotten.status_code == 200:
                pass
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_project_gotten.status_code,
                    "error_msg": res_project_gotten.text + "------------" + get_project_url,
                    "payload": str(get_project_json)
                }
            project_id = res_project_gotten.json()[0]['id']
            # get parent node
            parent_query = {
                "global_entity_id": geid,
            }
            res_parent_gotten = http_query_node("File", parent_query)
            print("res_parent_gotten", res_parent_gotten.text)
            if res_parent_gotten.status_code == 200:
                print('updated res_parent_gotten: ', res_parent_gotten.json())
            else:
                return {
                    "error": "archive meta in neo4j failed when getting parent",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query),
                    "url": res_parent_gotten.url
                }
            if res_parent_gotten.status_code == 200:
                print('add_approval_copy_for_neo4j res_parent_gotten: ',
                      res_parent_gotten.json())
            else:
                return {
                    "error": "add_approval_copy_for_neo4j in neo4j failed",
                    "errorcode": res_parent_gotten.status_code,
                    "error_msg": res_parent_gotten.text,
                    "payload": str(parent_query)
                }
            parent_node = res_parent_gotten.json()[0]
            parent_geid = parent_node['global_entity_id']
            file_tags = parent_node['tags']
            if not file_tags:
                file_tags = []
            # add approval copy tag
            add_url = ConfigClass.DATA_OPS_GR_V2 + \
                "containers/{}/tags".format(project_id)
            file_tags.append(ConfigClass.copied_with_approval)
            add_post = {
                "taglist": file_tags,
                "geid": parent_geid,
                "internal": True
            }
            respon_add_copy_tag = requests.post(add_url, json=add_post)
            if respon_add_copy_tag.status_code == 200:
                return "Succeed"
            else:
                return {
                    "error": "add_approval_copy_for_neo4ja in neo4j failed",
                    "errorcode": respon_add_copy_tag.status_code,
                    "error_msg": respon_add_copy_tag.text + "------------" + add_url,
                    "payload": str(add_post)
                }

        except Exception as e:
            return {
                "error": "archive meta in neo4j failed",
                "errorcode": 500,
                "error_msg": str(e)
            }

    def create_trash_node_in_neo4j(self, input_path, trash_path, trash_geid):
        ConfigClass = config_singleton()
        trash_url = ConfigClass.ENTITY_INFO_SERVICE + "files/trash"
        payload = {
            "trash_full_path": trash_path,
            "full_path": input_path,
            "trash_geid": trash_geid,
        }
        res = requests.post(url=trash_url, json=payload)
        if res.status_code == 200:
            print('create_trash_node_in_neo4j: ', res.json())
        else:
            print({
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            })
            return {
                "error": "create_trash_node_in_neo4j failed",
                "errorcode": res.status_code,
                "error_msg": res.text + "-----------" + trash_url,
                "request_body": str(payload)
            }

    def fetch_guid(self):
        ConfigClass = config_singleton()
        # fetch global entity id
        entity_id_url = ConfigClass.UTILITY_SERVICE + "/v1/utility/id?entity_type=file_data"
        respon_entity_id_fetched = requests.get(entity_id_url)
        if respon_entity_id_fetched.status_code == 200:
            pass
        else:
            raise Exception('Entity id fetch failed: ' + entity_id_url +
                            ": " + str(respon_entity_id_fetched.text))
        trash_geid = respon_entity_id_fetched.json()['result']
        return trash_geid