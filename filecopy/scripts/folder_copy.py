from config import config_singleton, set_config, config_factory
import os
import argparse
import time
import requests
import traceback
# import datetime

from utils import update_job, get_job, get_session_id
from minio_client import Minio_Client, Minio_Client_

from utils import get_resource_by_geid, http_query_node, http_update_node,\
    lock_resource, unlock_resource, debug_message_sender, logger_info
from utils import deprecate_index_in_es, create_lineage_v3, update_file_operation_logs, \
    create_es_search_index, create_catalog_entity


from neo4j_helper import get_node_by_geid, get_parent_node, \
    get_children_nodes, delete_relation_bw_nodes, delete_node, create_file_node, \
    create_folder_node

# from folder import FolderMgr
from copy import deepcopy

ConfigClass = None

PROCESS_PIPELINE = "data_transfer_folder"
PIPELINE_DESC = '''
    the script will copy the folder from greenroom to core recursively
'''

#####################################################################################

class CopyObjects():
    def __init__(self, minio_client, project, oper):
        self.mc = minio_client
        self.project = project
        self.oper = oper

    def recursive_copy(self, currenct_nodes, current_root_path, parent_node, new_name=None):
        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:
            print(ff_object)
            ff_geid = ff_object.get("global_entity_id")
            # update here if the folder/file is archieved then skip
            if ff_object.get("archived", False):
                continue
            
            ################################################################################################

            # recursive logic below
            if 'File' in ff_object.get("labels"):
                # TODO simplify here
                minio_path = ff_object.get('location').split("//")[-1]
                _, bucket, old_path = tuple(minio_path.split("/", 2))

                # # lock the resource
                lockkey_template = "{}/{}"
                old_lockkey = "{}/{}".format(bucket, old_path)
                new_lockkey = lockkey_template.format(
                    current_root_path, new_name if new_name else ff_object.get("name"))
                
                # try to aquire the lock for old path and lock the new resources
                lock_resource(old_lockkey)
                lock_resource(new_lockkey)
                
                # file will need extra step to get all attribute
                # the format of attribute is {"attr_<field>": "value"}
                attr = {x:ff_object[x] for x in ff_object if "attr" in x}
                tags = ff_object.get("tags")
                extra = {"system_tags": ["copied-to-core"]}
                print(extra)
                # create the copied node
                # TODO add the guid to the new node
                new_node, _ = create_file_node(self.project.get("code"), ff_object, self.oper, parent_node.get('id'), \
                    current_root_path, self.mc, tags=tags, attribute=attr, new_name=new_name, extra_fields=extra) 

                ################################## Metadata Generating ###################################
                source_geid = ff_object.get("global_entity_id")
                target_geid = new_node.get("global_entity_id")
                project_code = self.project.get("code")
                # also transfer the saved preview info to copied one
                copy_zippreview(source_geid, target_geid)

                # create the new node in atlas for lineage linking
                guid = create_catalog_entity(new_node, self.oper)

                # create the lineage link between greenroom -> relation -> core
                create_lineage_v3(source_geid, target_geid, project_code, PROCESS_PIPELINE,
                    PIPELINE_DESC)

                # create the elastic search index for advance search
                create_es_search_index(new_node, ff_object, "File", 
                    self.project["id"], "vrecore", PROCESS_PIPELINE, guid)

                # # create the file stream/operational logs index in elastic search
                # print("====== create activity log")
                res_update_audit_logs = update_file_operation_logs(
                    ff_object.get('uploader'), self.oper,
                    os.path.join('Greenroom', ff_object.get("display_path", "")),
                    os.path.join('VRECore', new_node.get("display_path", "")),
                    ff_object.get('file_size', 0),
                    project_code,
                    ff_object.get("generate_id", "undefined")
                )
                logger_info('res_update_audit_logs: ' +
                    str(res_update_audit_logs.status_code))

                unlock_resource(old_lockkey)
                unlock_resource(new_lockkey)

            # else it is folder will trigger the recursive
            elif 'Folder' in ff_object.get("labels"):
                
                # first create the folder
                tags = ff_object.get("tags")
                extra = {"system_tags": ["copied-to-core"]}
                new_node, _ = create_folder_node(self.project.get("code"), ff_object, self.oper, \
                    parent_node, current_root_path, tags=tags, new_name=new_name, extra_fields=extra)

                print(new_node)
                
                # metadata creation
                create_es_search_index(new_node, ff_object, "Folder", 
                    self.project["id"], "vrecore", PROCESS_PIPELINE, "")

                # seconds recursively go throught the folder/subfolder by same proccess
                # also if we want the folder to be renamed if new_name is not None
                next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_geid)
                self.recursive_copy(children_nodes, next_root, new_node)

        return 


#####################################################################################

def recursive_copy(currenct_nodes, dataset, oper, current_root_path, \
    parent_node, minio_client:Minio_Client_, job_tracker=None, new_name=None):

    num_of_files = 0
    total_file_size = 0
    # this variable DOESNOT contain the child nodes
    new_lv1_nodes = []

    # copy the files under the project neo4j node to dataset node
    for ff_object in currenct_nodes:
        ff_geid = ff_object.get("global_entity_id")
        # new_node = None

        # update here if the folder/file is archieved then skip
        if ff_object.get("archived", False):
            continue
        
        ################################################################################################
        print(ff_object)

        # recursive logic below
        if 'File' in ff_object.get("labels"):
            # TODO simplify here
            minio_path = ff_object.get('location').split("//")[-1]
            _, bucket, old_path = tuple(minio_path.split("/", 2))

            # # lock the resource
            lockkey_template = "{}/{}"
            old_lockkey = "{}/{}".format(bucket, old_path)
            new_lockkey = "{}/{}/{}".format('core-'+dataset.get("code"), current_root_path,
                new_name if new_name else ff_object.get("name"))
            
            # # try to aquire the lock for old path and lock the new resources
            lock_resource(old_lockkey)
            lock_resource(new_lockkey)
            
            # file will need extra step to get all attribute
            # the format of attribute is {"attr_<field>": "value"}
            attr = {x:ff_object[x] for x in ff_object if "attr" in x}
            tags = ff_object.get("tags")
            # create the copied node
            # TODO add the guid to the new node
            new_node, _ = create_file_node(dataset.get("code"), ff_object, oper, parent_node.get('id'), \
                current_root_path, minio_client, tags=tags, attribute=attr, new_name=new_name) 

            # add system_tags for old nodes
            update_json = {"system_tags": ["copied-to-core"]}
            http_update_node("File", ff_object.get("id"), update_json)

            ################################## Metadata Generating ###################################
            source_geid = ff_object.get("global_entity_id")
            target_geid = new_node.get("global_entity_id")
            project_code = dataset.get("code")
            # unix_process_time = datetime.datetime.utcnow().timestamp()
            # also transfer the saved preview info to copied one
            copy_zippreview(source_geid, target_geid)

            # create the new node in atlas for lineage linking
            # print("====== create entity in atlas")
            guid = create_catalog_entity(new_node, oper)

            # create the lineage link between greenroom -> relation -> core
            # print("====== create lineage in atlas")
            create_lineage_v3(source_geid, target_geid, project_code, PROCESS_PIPELINE,
                PIPELINE_DESC)

            # create the elastic search index for advance search
            # print("====== create search index in ES")
            # new_node.update({"project_id": dataset["id"]})
            # new_node.update({"full_path": new_node.get("location")})
            # new_node.update({"generate_id": ff_object.get("generate_id", None)})
            # new_node.update({"guid": guid})
            # new_node.update({"zone":"vrecore"})
            # new_node.update({"atlas_guid": new_node.get("guid")})
            # new_node.update({"process_pipeline": PROCESS_PIPELINE})
            create_es_search_index(new_node, ff_object, "File", 
                dataset["id"], "vrecore", PROCESS_PIPELINE, guid)

            # # create the file stream/operational logs index in elastic search
            # print("====== create activity log")
            res_update_audit_logs = update_file_operation_logs(
                ff_object.get('uploader'), oper,
                os.path.join('Greenroom', ff_object.get("display_path", "")),
                os.path.join('VRECore', new_node.get("display_path", "")),
                ff_object.get('file_size', 0),
                project_code,
                ff_object.get("generate_id", "undefined")
            )
            logger_info('res_update_audit_logs: ' +
                str(res_update_audit_logs.status_code))

            unlock_resource(old_lockkey)
            unlock_resource(new_lockkey)

        # else it is folder will trigger the recursive
        elif 'Folder' in ff_object.get("labels"):
            
            # lock the resource
            lockkey_template = "{}/{}"
            old_lockkey = lockkey_template.format('gr-'+dataset.get("code"), ff_object.get("display_path"))
            new_lockkey = "{}/{}/{}".format('core-'+dataset.get("code"), current_root_path,
                new_name if new_name else ff_object.get("name"))
            
            # # try to aquire the lock for old path and lock the new resources
            lock_resource(old_lockkey)
            lock_resource(new_lockkey)

            # first create the folder
            tags = ff_object.get("tags")
            new_node, _ = create_folder_node(dataset.get("code"), ff_object, oper, \
                parent_node, current_root_path, tags, new_name)
            
            # metadata creation
            # new_node.update({
            #     "data_type": "Folder",
            #     "archived": False,
            #     "location": "",
            #     "process_pipeline": PROCESS_PIPELINE,
            #     "file_name": new_node.get("name"),
            #     "atlas_guid": "",
            #     "generate_id": None,
            #     "zone":"vrecore",
            #     "operator": oper,
            #     "uploader": oper,
            #     "file_size": 0
            # })
            create_es_search_index(new_node, ff_object, "Folder", 
                dataset["id"], "vrecore", PROCESS_PIPELINE, "")

            # seconds recursively go throught the folder/subfolder by same proccess
            # also if we want the folder to be renamed if new_name is not None
            next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
            children_nodes = get_children_nodes(ff_geid)
            num_of_child_files, num_of_child_size, _ = \
                recursive_copy(children_nodes, dataset, oper, next_root, new_node, \
                    minio_client)

            update_json = {"system_tags": ["copied-to-core"]}
            http_update_node("Folder", ff_object.get("id"), update_json)

            unlock_resource(old_lockkey)
            unlock_resource(new_lockkey)


    return num_of_files, total_file_size, new_lv1_nodes


def copy_execute(job_id, new_name, dest_geid, input_geid, project_code, operator, auth_token: dict):
    source_folder_node = get_resource_by_geid(input_geid)
    print(source_folder_node)
    dest_node = get_resource_by_geid(dest_geid)
    print(dest_node)

    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    uploader = source_folder_node.get("uploader", "admin")

    print(" - target node:", dest_node)
    print()
    print(" - project node:", project_info)
    print("======")

    # lock the destination if it is NOT name folder
    if dest_node.get("name") != dest_node.get("uploader"):
        lockkey_template = "{}/{}"
        new_lockkey = lockkey_template.format('core-'+project_info.get("code"), dest_node.get("display_path"))
        lock_resource(new_lockkey)

    # initialize the minio outside to keep one instance of credential
    # if dont do so, the large folder will cause the initial token expire
    mc = Minio_Client_(ConfigClass, auth_token["at"], auth_token["rt"])

    # copy_object = CopyObjects(mc, project_info, operator)
    # copy_object.recursive_copy([source_folder_node], dest_node.get("display_path"), \
    #     dest_node, new_name=new_name)

    recursive_copy([source_folder_node], project_info, operator, dest_node.get("display_path"), \
        dest_node, mc, new_name=new_name)

    # lock the destination if it is NOT name folder
    if dest_node.get("name") != dest_node.get("uploader"):
        unlock_resource(new_lockkey)


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

#################################################### Main ##############################################

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

        # logger_info('all varible: ' + str(args))
        logger_info('project_code: ' + project_code)
        logger_info('_config environment: ' + str(_config.env))
        logger_info('output_geid: ' + output_geid)
        logger_info('input_geid: ' + input_geid)
        logger_info('operator: ' + operator)
        logger_info('job_id: ' + job_id)
        logger_info('rename: ' + rename)

        logger_info('====== Start Copy')

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
