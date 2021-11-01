# import datetime
import os 
import argparse
import time
import requests
import traceback
import re
import json

# from folder import http_query_node, http_query_node_zone
from utils import get_resource_by_geid, http_update_node, http_query_node,\
    lock_resource, unlock_resource, debug_message_sender, logger_info
from utils import update_job, get_job, get_session_id
from utils import deprecate_index_in_es, create_lineage_v3, update_file_operation_logs, \
    create_es_search_index, create_catalog_entity

from config import config_singleton, set_config, config_factory
from minio_client import Minio_Client, Minio_Client_

from neo4j_helper import get_node_by_geid, get_parent_node, \
    get_children_nodes, delete_relation_bw_nodes, delete_node, create_file_node, \
    create_folder_node, archived_file_node


ConfigClass = None

PROCESS_PIPELINE = "data_delete_folder"
PIPELINE_DESC = '''
    the script will delete the folder in greenroom/core recursively
'''


#################################################################################################################
class DeleteObjects():
    def __init__(self, minio_client, project, oper):
        self.mc = minio_client
        self.project = project
        self.oper = oper

    def recursive_delete(self, currenct_nodes, current_root_path, parent_node, new_name=None):
        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:
            ff_geid = ff_object.get("global_entity_id")
            # TODO update here
            zone, zone_label = ("greenroom", "Greenroom") if "Greenroom" in ff_object.get("labels") \
                else ("vrecore", "VRECore")

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
                # lockkey_template = "{}/{}"
                # old_lockkey = "{}/{}".format(bucket, old_path)
                # new_lockkey = lockkey_template.format(
                #     current_root_path, new_name if new_name else ff_object.get("name"))
                
                # # try to aquire the lock for old path and lock the new resources
                # lock_resource(old_lockkey)
                # lock_resource(new_lockkey)

                
                # file will need extra step to get all attribute
                # the format of attribute is {"attr_<field>": "value"}
                attr = {x:ff_object[x] for x in ff_object if "attr" in x}
                # TODO move the other place
                extra_fields = {"archived":True, "in_trashbin":True, 
                    "list_priority":parent_node.get("list_priority", 10)}
                tags = ff_object.get("tags")
                # create the copied node
                new_node, _ = archived_file_node(self.project.get("code"), ff_object, \
                    self.oper, parent_node.get('id'), current_root_path, self.mc, tags=tags, \
                    attribute=attr, new_name=new_name, extra_labels=["TrashFile", zone_label], \
                    extra_fields=extra_fields) 


                ################################## Metadata Generating ###################################
                source_geid = ff_object.get("global_entity_id")
                target_geid = new_node.get("global_entity_id")
                project_code = self.project.get("code")
                # unix_process_time = datetime.datetime.utcnow().timestamp()

                # create the new node in atlas for lineage linking
                guid = create_catalog_entity(new_node, self.oper)

                # create the lineage link between greenroom -> relation -> core
                create_lineage_v3(source_geid, target_geid, project_code, PROCESS_PIPELINE,
                    PIPELINE_DESC)

                # deprecate old node in es
                deprecate_index_in_es(ff_object.get("global_entity_id"))

                # create the file stream/operational logs index in elastic search
                res_update_audit_logs = update_file_operation_logs(
                    ff_object.get('uploader'), self.oper,
                    os.path.join('Greenroom', ff_object.get("display_path", "")),
                    os.path.join('VRECore', new_node.get("display_path", "")),
                    ff_object.get('file_size', 0),
                    project_code,
                    ff_object.get("generate_id", "undefined"),
                    operation_type="data_delete"
                )
                logger_info('res_update_audit_logs: ' +
                    str(res_update_audit_logs.status_code))

                # update the old node to archived
                update_json = {'archived': True}
                # if new_name: update_json.update({"name": new_name})
                http_update_node("File", ff_object.get("id"), update_json)


                # unlock_resource(old_lockkey)
                # unlock_resource(new_lockkey)

            # else it is folder will trigger the recursive
            elif 'Folder' in ff_object.get("labels"):

                # first create the trash folder node
                extra_fields = {"archived":True, "in_trashbin":True, 
                    "list_priority":parent_node.get("list_priority", 20)}
                tags = ff_object.get("tags")
                new_node, _ = create_folder_node(self.project.get("code"), ff_object, self.oper, \
                    parent_node, current_root_path, tags=tags, new_name=new_name, \
                    extra_labels=["TrashFile", zone_label], extra_fields=extra_fields)

                # deprecate old node in es
                deprecate_index_in_es(ff_object.get("global_entity_id"))
                
                # seconds recursively go throught the folder/subfolder by same proccess
                # also if we want the folder to be renamed if new_name is not None
                next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_geid)
                self.recursive_delete(children_nodes, next_root, new_node)

                # update the old node to archived
                update_json = {'archived': True}
                # if new_name: update_json.update({"name": new_name})
                http_update_node("Folder", ff_object.get("id"), update_json)

        return 


#################################################################################################################


def recursive_copy(currenct_nodes, dataset, oper, current_root_path, \
    parent_node, minio_client:Minio_Client_, job_tracker=None, new_name=None):
    '''
        This is a recursive function. When it detects the folder will continue
        expand to its children nodes(ignoring archived node).
    '''

    num_of_files = 0
    total_file_size = 0
    # this variable DOESNOT contain the child nodes
    new_lv1_nodes = []

    # copy the files under the project neo4j node to dataset node
    for ff_object in currenct_nodes:
        ff_geid = ff_object.get("global_entity_id")
        new_node = None
        # TODO update here
        zone, zone_label, bucket_prefix = ("greenroom", "Greenroom", "gr-") \
            if "Greenroom" in ff_object.get("labels") \
            else ("vrecore", "VRECore", "core-")

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

            # lock the resource
            lockkey_template = "{}/{}"
            old_lockkey = "{}/{}".format(bucket, old_path)
            
            # try to aquire the lock for old path and lock the new resources
            lock_resource(old_lockkey)

            
            # file will need extra step to get all attribute
            # the format of attribute is {"attr_<field>": "value"}
            attr = {x:ff_object[x] for x in ff_object if "attr" in x}
            # TODO move the other place
            extra_fields = {"archived":True, "in_trashbin":True}
            tags = ff_object.get("tags")
            # create the copied node
            new_node, _ = archived_file_node(dataset.get("code"), ff_object, oper, parent_node.get('id'), \
                current_root_path, minio_client, tags=tags, attribute=attr, \
                new_name=new_name, extra_labels=["TrashFile", zone_label], extra_fields=extra_fields) 


            ################################## Metadata Generating ###################################
            source_geid = ff_object.get("global_entity_id")
            target_geid = new_node.get("global_entity_id")
            project_code = dataset.get("code")
            # unix_process_time = datetime.datetime.utcnow().timestamp()

            # create the new node in atlas for lineage linking
            guid = create_catalog_entity(new_node, oper)

            # create the lineage link between greenroom -> relation -> core
            create_lineage_v3(source_geid, target_geid, project_code, PROCESS_PIPELINE,
                PIPELINE_DESC)

            # deprecate old node in es
            deprecate_index_in_es(ff_object.get("global_entity_id"))
            

            # # create the file stream/operational logs index in elastic search
            res_update_audit_logs = update_file_operation_logs(
                ff_object.get('uploader'), oper,
                os.path.join('Greenroom', ff_object.get("display_path", "")),
                os.path.join('VRECore', new_node.get("display_path", "")),
                ff_object.get('file_size', 0),
                project_code,
                ff_object.get("generate_id", "undefined"),
                operation_type="data_delete"
            )
            logger_info('res_update_audit_logs: ' +
                str(res_update_audit_logs.status_code))

            # update the old node to archived
            update_json = {'archived': True}
            if new_name: update_json.update({"name": new_name})
            http_update_node("File", ff_object.get("id"), update_json)


            unlock_resource(old_lockkey)

        # else it is folder will trigger the recursive
        elif 'Folder' in ff_object.get("labels"):

            # lock the resource
            lockkey_template = "{}/{}"
            old_lockkey = lockkey_template.format(bucket_prefix+dataset.get("code"), ff_object.get("display_path"))
            
            # try to aquire the lock for old path and lock the new resources
            lock_resource(old_lockkey)


            extra_fields = {"archived":True, "in_trashbin":True}
            # first create the folder
            tags = ff_object.get("tags")
            new_node, _ = create_folder_node(dataset.get("code"), ff_object, oper, \
                parent_node, current_root_path, tags=tags, new_name=new_name, \
                extra_labels=["TrashFile", zone_label], extra_fields=extra_fields)

            # deprecate old node in es
            print("====== deprecate search index in ES")
            deprecate_index_in_es(ff_object.get("global_entity_id"))
            
            # seconds recursively go throught the folder/subfolder by same proccess
            # also if we want the folder to be renamed if new_name is not None
            next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
            children_nodes = get_children_nodes(ff_geid)
            num_of_child_files, num_of_child_size, _ = \
                recursive_copy(children_nodes, dataset, oper, next_root, new_node, \
                    minio_client)

            # update the old node to archived
            update_json = {'archived': True}
            if new_name: update_json.update({"name": new_name})
            http_update_node("Folder", ff_object.get("id"), update_json)


        ##########################################################################################################
    return num_of_files, total_file_size, new_lv1_nodes


def dele_execute(job_id, input_geid, project_code, operator, auth_token: dict):
    '''
        Entry point for the deletion logic. inside function, it will do some
        paperation(eg. fecthing necessary infomation), then calling recursive
        function recursive_copy to archive the input nodes.
    '''
    print("====== Delete Start")
    source_folder_node = get_resource_by_geid(input_geid)
    print("source:", source_folder_node)
    
    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    output_folder_name = source_folder_node['name'] + "_" + str(round(time.time()))

    # initialize the minio outside to keep one instance of credential
    # if dont do so, the large folder will cause the initial token expire
    mc = Minio_Client_(ConfigClass, auth_token["at"], auth_token["rt"])
    # delete_object = DeleteObjects(mc, project_info, operator)
    # delete_object.recursive_delete([source_folder_node], source_folder_node.get("uploader"), \
    #     project_info, new_name=output_folder_name)

    # the deleted folder will be attached directly under the project
    recursive_copy([source_folder_node], project_info, operator, source_folder_node.get("uploader"), \
        project_info, mc, new_name=output_folder_name)

 
######################################### Main ########################################

def parse_inputs():
    parser = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-i', '--input-geid', help='Sepecify input file', 
                        metavar='FILE/Folder', required=True)
    parser.add_argument('-env', '--environment',
                        help='Environment', required=True)
    parser.add_argument('-p', '--project-code',
                        help='Project code', required=True)
    parser.add_argument('-t', '--trash-path', help='Trash folder path', 
                        metavar='PATH', required=True)
    parser.add_argument('-op', '--operator',
                        help='Action operator', required=True)
    parser.add_argument('-j', '--job-id',
                        help='Job geid', required=True)
    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def main():
    global ConfigClass
    try:
        # fecthing all the varibale/parameter from the script args
        environment = args.get('environment', 'test')
        set_config(config_factory(environment))
        _config = config_singleton(environment)
        ConfigClass = _config

        job_id = args['job_id']
        input_geid = args['input_geid']
        project_code = args['project_code']
        operator = args['operator']
        session_id = get_session_id(job_id)

        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        logger_info('_config environment: ' + str(_config.env))

        # add new variable for the minio token
        token = {
            "at": args['access_token'],
            "rt": args['refresh_token']
        }
        try:
            dele_execute(job_id, input_geid, project_code, operator, token)
            update_job(session_id, job_id, 'SUCCEED')
            logger_info(f'Successfully moved file from {input_geid} ')

        except Exception as e:
            error_msg = f'Failed to move file from {input_geid} \n {e}'
            update_job(session_id, job_id, 'TERMINATED', add_payload={'error_msg': str(e)})
            logger_info(error_msg)
            raise
    except Exception as e:
        raise

if __name__ == "__main__":
    try:
        args = parse_inputs()
        logger_info(args)
        main()
    except Exception as e:
        logger_info("[Delete Failed] {}".format(str(e)))
        for info in traceback.format_stack():
            logger_info(info)
        raise

