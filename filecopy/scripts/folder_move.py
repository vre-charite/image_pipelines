import argparse
import os
import traceback

from config import ConfigClass
from minio_client import Minio_Client_
from models import append_suffix_to_filepath
from models import get_timestamp
from models import Node
from models import ResourceType
from neo4j_helper import archived_file_node
from neo4j_helper import create_folder_node
from neo4j_helper import get_children_nodes
from utils import get_resource_by_geid
from utils import get_session_id
from utils import http_query_node
from utils import http_update_node
from utils import lock_resource
from utils import logger_info
from utils import MetaDataFactory
from utils import unlock_resource
from utils import update_job

PROCESS_PIPELINE = "data_delete_folder"
PIPELINE_DESC = '''
    the script will delete the folder in greenroom/core recursively
'''
OPERATION_TYPE = "data_delete"


class DeleteObjects:
    def __init__(self, minio_client, metadata_factory):
        self.mc = minio_client
        self.metadata_factory = metadata_factory

        self.project = self.metadata_factory.project
        self.oper = self.metadata_factory.oper
        self.zone_label = self.metadata_factory.zone_label

    def recursive_delete(self, currenct_nodes, current_root_path, parent_node: Node, new_name=None):
        # copy the files under the project neo4j node to dataset node
        for ff_object in currenct_nodes:
            ff_geid = ff_object.get("global_entity_id")
            # TODO update here
            # zone, zone_label = ("greenroom", "Greenroom") if "Greenroom" in ff_object.get("labels") \
            # else ("vrecore", "VRECore")

            # update here if the folder/file is archieved then skip
            if ff_object.get("archived", False):
                continue

            print(ff_object)

            # recursive logic below
            if 'File' in ff_object.get("labels"):
                # TODO simplify here
                minio_path = ff_object.get('location').split("//")[-1]
                _, bucket, old_path = tuple(minio_path.split("/", 2))

                # file will need extra step to get all attribute
                # the format of attribute is {"attr_<field>": "value"}
                attr = {x: ff_object[x] for x in ff_object if "attr" in x}
                # TODO move the other place
                extra_fields = {
                    "archived": True,
                    "in_trashbin": True,
                    "list_priority": parent_node.get("list_priority", 10),
                }
                tags = ff_object.get("tags")
                # create the copied node
                new_node, _ = archived_file_node(
                    self.project.get("code"),
                    ff_object,
                    self.oper,
                    parent_node.get('id'),
                    current_root_path,
                    self.mc,
                    tags=tags,
                    attribute=attr,
                    new_name=new_name,
                    extra_labels=[ResourceType.TRASH_FILE, self.metadata_factory.zone_label],
                    extra_fields=extra_fields,
                )

                ################################## Metadata Generating ###################################
                source_geid = ff_object.get("global_entity_id")
                target_geid = new_node.get("global_entity_id")

                # create the new node in atlas for lineage linking
                guid = self.metadata_factory.create_catalog_entity(new_node)

                # create the lineage link between greenroom -> relation -> core
                self.metadata_factory.create_lineage_v3(source_geid, target_geid)

                # deprecate old node in es
                self.metadata_factory.deprecate_index_in_es(ff_object.get("global_entity_id"))

                # create the file stream/operational logs index in elastic search
                res_update_audit_logs = self.metadata_factory.update_file_operation_logs(
                    os.path.join(self.zone_label, ff_object.get("display_path", "")),
                    os.path.join(self.zone_label, new_node.get("display_path", ""))
                )
                logger_info('res_update_audit_logs: ' + str(res_update_audit_logs.status_code))

                # update the old node to archived
                update_json = {'archived': True}
                http_update_node("File", ff_object.get("id"), update_json)

            # else it is folder will trigger the recursive
            elif 'Folder' in ff_object.get("labels"):

                # first create the trash folder node
                extra_fields = {
                    "archived": True,
                    "in_trashbin": True,
                    "list_priority": parent_node.get("list_priority", 20),
                }
                tags = ff_object.get("tags")
                new_node, _ = create_folder_node(
                    self.project.get("code"),
                    ff_object,
                    self.metadata_factory.oper,
                    parent_node,
                    current_root_path,
                    tags=tags,
                    new_name=new_name,
                    extra_labels=[ResourceType.TRASH_FILE, self.zone_label],
                    extra_fields=extra_fields,
                )

                # deprecate old node in es
                self.metadata_factory.deprecate_index_in_es(ff_object.get("global_entity_id"))

                # seconds recursively go throught the folder/subfolder by same proccess
                # also if we want the folder to be renamed if new_name is not None
                next_root = current_root_path + "/" + (new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_geid)
                self.recursive_delete(children_nodes, next_root, new_node)

                # update the old node to archived
                update_json = {'archived': True}
                http_update_node("Folder", ff_object.get("id"), update_json)

        return

    #################################################################################################################


# TODO somehow refactory here?
def recursive_lock(code, nodes, zone):
    """Function will recursively lock the node tree."""

    bucket_prefix = "gr-" if zone == "greenroom" else "core-"
    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    def recur_walker(currenct_nodes):
        """Recursively trace down the node tree and run the lock function."""

        for ff_object in currenct_nodes:
            # we will skip the deleted nodes
            if ff_object.get("archived", False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source and target
            if ff_object.get("display_path") != ff_object.get("uploader"):
                source_key = "{}/{}".format(bucket_prefix + code, ff_object.get("display_path"))
                lock_resource(source_key, "write")
                locked_node.append((source_key, "write"))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get("labels"):
                children_nodes = get_children_nodes(ff_object.get("global_entity_id", None))
                recur_walker(children_nodes)

        return

    # start here
    try:
        recur_walker(nodes)
    except Exception as e:
        err = e

    return locked_node, err


def delete_execute(job_id, input_geid, project_code, operator, auth_token: dict):
    """Entry point for the deletion logic. inside function, it will do some
    paperation(eg. fecthing necessary infomation), then calling recursive
    function recursive_copy to archive the input nodes."""

    print("====== Delete Start")
    source_node = get_resource_by_geid(input_geid)
    print("source:", source_node)

    # get project
    project_response = http_query_node('Container', {"code": project_code})
    project_info = project_response.json()[0]
    output_folder_name = append_suffix_to_filepath(source_node['name'], get_timestamp())

    locked_node = []
    try:
        zone = "greenroom" if "Greenroom" in source_node.get("labels") else "vrecore"
        # at begining lock the whole node tree
        locked_node, err = recursive_lock(project_info.get("code"), [source_node], zone)
        if err:
            raise err

        # initialize the minio outside to keep one instance of credential
        # if dont do so, the large folder will cause the initial token expire
        mc = Minio_Client_(auth_token["at"], auth_token["rt"])

        metadata_factory = MetaDataFactory(
            project_info, operator, zone, PROCESS_PIPELINE, PIPELINE_DESC, OPERATION_TYPE
        )

        delete_object = DeleteObjects(mc, metadata_factory)
        delete_object.recursive_delete(
            [source_node], source_node.get("uploader"), project_info, new_name=output_folder_name
        )
    except Exception as e:
        raise e
    finally:
        # here we unlock the locked nodes ONLY
        print("Start to unlock the nodes")
        for resource_key, operation in locked_node:
            unlock_resource(resource_key, operation)


def parse_inputs():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
    try:
        # fecthing all the varibale/parameter from the script args
        environment = args.get('environment', 'test')

        job_id = args['job_id']
        input_geid = args['input_geid']
        project_code = args['project_code']
        operator = args['operator']
        session_id = get_session_id(job_id)

        logger_info('environment: ' + str(args.get('environment')))
        logger_info('config set: ' + environment)
        logger_info('_config environment: ' + str(ConfigClass.env))

        # add new variable for the minio token
        token = {
            "at": args['access_token'],
            "rt": args['refresh_token']
        }
        try:
            delete_execute(job_id, input_geid, project_code, operator, token)
            update_job(session_id, job_id, 'SUCCEED')
            logger_info(f'Successfully moved file from {input_geid} ')

        except Exception as e:
            error_msg = f'Failed to move file from {input_geid} \n {e}'
            update_job(session_id, job_id, 'TERMINATED', add_payload={'error_msg': str(e)})
            logger_info(error_msg)
            raise
    except Exception:
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
