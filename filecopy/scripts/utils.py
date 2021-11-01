from config import config_singleton
import requests
import re
import os

import datetime

ConfigClass = config_singleton()

###################################################### Utility ##################################################

def http_query_node(primary_label, query_params={}):
    '''
    primary_label i.e. Folder, File, Container
    '''
    # ConfigClass = config_singleton()
    payload = {
        **query_params
    }
    node_query_url = ConfigClass.NEO4J_SERVICE + "nodes/{}/query".format(primary_label)
    response = requests.post(node_query_url, json=payload)
    return response

def get_resource_by_geid(geid):
    '''
        function will call the neo4j api to get the node
        by geid. raise exception if the geid is not exist
    '''
    url = ConfigClass.NEO4J_SERVICE + "nodes/geid/%s"%geid
    res = requests.get(url)
    nodes = res.json()

    if len(nodes) == 0:
        raise Exception('Not found resource: ' + geid)

    return nodes[0]


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    # ConfigClass = config_singleton()
    update_url = ConfigClass.NEO4J_SERVICE + \
        "nodes/{}/node/{}".format(primary_label, neo4j_id)
    res = requests.put(url=update_url, json=update_json)
    return res


def lock_resource(resource_key):
    print("====== Lock resource")
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {"resource_key": resource_key}

    # first see if the resource is locked
    response = requests.get(url, params=post_json)
    if response.json().get("result", {}).get("status", "LOCKED") == "LOCKED":
        raise Exception("resource %s already in used"%resource_key)

    response = requests.post(url, json=post_json)
    return response


def unlock_resource(resource_key):
    print("====== Lock resource")
    # ConfigClass = config_singleton()
    url = ConfigClass.DATA_OPS_UT + 'resource/lock'
    post_json = {
        "resource_key": resource_key
    }
    response = requests.delete(url, json=post_json)
    print(response.json())
    return response


def debug_message_sender(message: str):
    # _config = config_singleton()
    url = ConfigClass.DATA_OPS_UT + "files/actions/message"
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


########################################### Metadata Creation #####################################################

def create_lineage_v3(input_geid, output_geid, projectCode, pipelineName, description, create_time=None):
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

    print("====== create lineage in atlas")

    # ConfigClass = config_singleton()
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


def update_file_operation_logs(owner, operator, input_file_path, output_file_path,
                               file_size, project_code, generate_id, operation_type="data_transfer", extra=None):
    '''
    Endpoint
    url_audit_log = ConfigClass.PROVENANCE_SERVICE + 'audit-logs'
    '''

    print("====== create activity log")

    # new audit log api
    # ConfigClass = config_singleton()
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

def deprecate_index_in_es(geid):
    print("====== deprecate search index in ES")
    es_payload = {
        "global_entity_id": geid,
        "updated_fields": {
            "archived": True,
        }
    }
    logger_info(f"es delete file payload: {es_payload}")
    es_res = requests.put(
        ConfigClass.PROVENANCE_SERVICE + 'entity/file', json=es_payload)
    logger_info(f"es delete trash file response: {es_res.text}")
    return es_res


def string_2_timestamp(time_string:str):
    return int(datetime.datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%S")\
        .replace(tzinfo=datetime.timezone.utc).timestamp())


def create_es_search_index(new_node, src_node, data_type, project_id, zone, 
    process_pipeline, guid):

    print("====== create search index in ES")

    # ConfigClass = config_singleton()

    # change string datetime into timestamp
    new_node["time_lastmodified"] = string_2_timestamp(new_node["time_lastmodified"])
    new_node["time_created"] = string_2_timestamp(new_node["time_created"])


    # new_node.update({"project_id": dataset["id"]})
    # new_node.update({"full_path": new_node.get("location")})
    # new_node.update({"generate_id": src_node.get("generate_id", None)})
    # new_node.update({"guid": guid})
    # new_node.update({"zone":"vrecore"})
    # new_node.update({"atlas_guid": new_node.get("guid")})
    # new_node.update({"process_pipeline": PROCESS_PIPELINE})
    # update some necessary field for index
    new_node.update({
        "data_type": data_type,
        "archived": False,
        "location": new_node.get("location", ""),
        "process_pipeline": process_pipeline,
        "file_name": new_node.get("name"),
        "guid": guid,
        "atlas_guid": guid,
        "generate_id": src_node.get("generate_id", None),
        "zone":zone,
        "operator": src_node.get("uploader", None),
        "uploader": src_node.get("uploader", None),
        "file_size": src_node.get("file_size", 0),
        "generate_id": src_node.get("generate_id", None),
        "full_path": new_node.get("location"),
    })

    # TODO create new api for this
    if "manifest_id" in new_node:
        manifest_id = new_node['manifest_id']
        full_path = new_node['full_path']

        attributes = []
        res = requests.get(
            ConfigClass.ENTITY_INFO_SERVICE + f"manifest/{manifest_id}")
        if res.status_code == 200:
            manifest_data = res.json()
            manifest = manifest_data['result']
            sql_attributes = manifest['attributes']

            for sql_attribute in sql_attributes:
                # mc will be the array list while others are string liked
                if sql_attribute["type"] == 'multiple_choice':
                    attribute_value = []
                    attribute_value.append(new_node.get(
                        "attr_" + sql_attribute['name'], ""))
                else:
                    attribute_value = new_node.get("attr_" + sql_attribute['name'], "")

                # then format the es search entity
                attributes.append({
                    "attribute_name": sql_attribute['name'],
                    "name": manifest['name'],
                    "value": attribute_value,
                })

        new_node.update({"attributes":attributes})


    es_res = requests.post(ConfigClass.PROVENANCE_SERVICE + 'entity/file', json=new_node)
    if es_res.status_code >= 300:
        raise Exception("Error in create_es_search_index "+str(es_res.json()))
    
    return es_res


# TODO need some refactory here
def create_catalog_entity(payload, oper):
    print("====== create entity in atlas")
    
    # add required field
    payload.update({"uploader": oper})
    payload.update({"file_name": payload.get("name")})
    payload.update({"path": payload.get("location")})
    payload.update({"namespace": "vrecore"})
    
    res = requests.post(url=ConfigClass.CATALOGUING_SERVICE_V2 + 'filedata', json=payload)


    if res.status_code == 200:
        json_payload = res.json()
        created_entity = None
        if json_payload['result']['mutatedEntities'].get('CREATE'):
            created_entity = json_payload['result']['mutatedEntities']['CREATE'][0]
        elif json_payload['result']['mutatedEntities'].get('UPDATE'):
            created_entity = json_payload['result']['mutatedEntities']['UPDATE'][0]
        if created_entity:
            guid = created_entity['guid']
            return guid

    raise Exception("error create_catalog_entity")
    return None


################################################### job functions #######################################

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

def get_job(job_id):
    url = ConfigClass.DATA_OPS_UT + "tasks"
    task_response = requests.get(
        url,
        params={
            "session_id": "*",
            "job_id": job_id
        }
    )
    my_task = task_response.json()['result'][0]
    return my_task

def get_session_id(job_id):
    job = get_job(job_id)
    return job["session_id"]