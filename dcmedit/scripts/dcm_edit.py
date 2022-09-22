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

'''
Tool to mask/remove dicom files

Created on Sep 5, 2016

@author: Shuai Liang
'''
import sys
import os
import argparse
import configparser
import glob
import zipfile
import datetime
import time
import subprocess
import shutil
import json
import io
import requests
from minio_client import Minio_Client_
from config import ConfigClass
from lock import ConfigClass, lock_resource, unlock_resource

from logger import Logger, LoggerException
import preprocess
from utils import MetaDataFactory, parse_zip, save_preview
from neo4j_helper import create_file_node, create_relation


PROCESS_PIPELINE = "dicom_edit"
PIPELINE_DESC = '''
    the script will produce the output from dicom zip
'''
OPERATION_TYPE = f"{ConfigClass.DCM_PROJECT}_pipeline"


def parse_inputs():
    parser = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-c', '--config-file', help='Secify config file', 
            metavar='FILE', required=False)
    parser.add_argument('-i', '--input-file', help='Minio location for Input zip file', 
            metavar='FILE', required=True)
    parser.add_argument('-o', '--output-dir', help='Minio location for Folder for output file',
             required=True)
    parser.add_argument('-t', '--workdir', help='Directory for intermediate files ',
             required=True)
    parser.add_argument('-l', '--log-file', help='Name of log file with full path',
             metavar='FILE', default=None)
    parser.add_argument('-p', '--project', help='Project ID')
    parser.add_argument('-s', '--subject', help='Subject ID')
    parser.add_argument('--use-default-anonymization', action='store_true')
    parser.add_argument('-a', '--anonymize-script', help='dicom edit script',
             metavar='FILE', default=None)
    parser.add_argument('-env', '--environment', help='environment', required=True)

    parser.add_argument('-at', '--access-token',
                        help='access key', required=True)
    parser.add_argument('-rt', '--refresh-token',
                        help='refresh key', required=True) 

    arguments = vars(parser.parse_args())
    if not arguments['log_file']: 
        arguments['log_file'] = "dcm_edit.log"
    if arguments['use_default_anonymization']:
        arguments['anonymize_script'] = 'dicomedit_scripts/anonymization.des'
    return arguments


def extract(fname, outdir):
    ref = zipfile.ZipFile(fname, 'r')
    ref.extractall(outdir)
    ref.close()
    LOGGER.info(f'extracted {fname} to {outdir}.')


def run_command(command):
    LOGGER.info(f'running {" ".join(command)}')
    res = subprocess.check_output(command)
    LOGGER.info(res)


def parse_location(path, env):
    protocol = "https://" if ConfigClass.MINIO_HTTPS else "http://"
    path = path.replace("minio://", "").replace(protocol, "").split("/")
    bucket = path[1]
    path = '/'.join(path[2:])
    return {"bucket": bucket, "path": path}


def download_file(minio_path, target_folder, env, mc: Minio_Client_):
    file_data = parse_location(minio_path, env)
    # for lock, since I know the dicom is zip so I will just lock the zip
    resource_key = "%s/%s"%(file_data.get("bucket"), file_data.get("path"))
    lock_resource(resource_key, "read")
    download_from_minio = datetime.datetime.now()
    LOGGER.info(f'Time after minio download: {download_from_minio}')

    target_path = None
    try:
        target_path = target_folder + "/" + file_data["path"].split("/")[-1]
        mc.client.fget_object(file_data["bucket"], file_data["path"], target_path)
    except Exception as e:
        raise e
    finally:
        # unlock it after download
        unlock_resource(resource_key, "read")
    
    return target_path


def upload_file(file_path, target_location, env, mc: Minio_Client_):
    file_data = parse_location(target_location, env)
    
    try:
        mc.client.fput_object(
            file_data["bucket"], 
            file_data["path"], 
            file_path, 
            metadata={"location": target_location.encode('utf-8')}
        )

        # here do another operation to get the size of uploaded file
        # we can use the os to get size also, but based on minio
        object_info = mc.client.stat_object(file_data["bucket"], file_data["path"])
    except Exception as e:
        raise e
    
    return object_info


def format_output_location(input_file):
    filename = os.path.basename(input_file)
    output_filename = filename.split(".")[0] + "_edited_"+str(int(time.time()))+".zip"
    return input_file.replace(filename, output_filename)
    

def main(env):   
    projects = [ConfigClass.DCM_PROJECT]
    if args['project'] not in projects and not args['anonymize_script']:
        raise ValueError(f"unknown project {args['project']}")
    t = datetime.datetime.now().strftime('%H%M%S') 
    t = os.path.basename(args['input_file']).split('.')[0] + t
    ext_dir = args['ext_dir'] = os.path.join(args['workdir'], t+'i')
    out_dir = os.path.join(args['workdir'], t+'o')
    output_location = format_output_location(args["input_file"])
    file_data = parse_location(output_location, env)
    resource_key = "%s/%s"%(file_data.get("bucket"), file_data.get("path"))

    token = {
        "at": args['access_token'],
        "rt": args['refresh_token']
    }
    LOGGER.info(token)

    try:
        os.makedirs(ext_dir) 
        LOGGER.info(f'extract dir: {ext_dir}')
        os.makedirs(out_dir)
        LOGGER.info(f'work dir: {out_dir}')

        # initialize the minio outside to keep one instance of credential
        # if dont do so, the dcmedit will cause the initial token expire
        mc = Minio_Client_(env, token['at'], token['rt'])

        download_zip_time = datetime.datetime.now()
        LOGGER.info(f'Start time of downloading the zip command time: {download_zip_time}')
        downloaded_file = download_file(args["input_file"], ext_dir, env, mc)
        after_download_zip_time = datetime.datetime.now()
        LOGGER.info(f'End time of downloading the zip command time: {after_download_zip_time}')

        print(downloaded_file)

        if args['project'] == ConfigClass.DCM_PROJECT:
            preprocess.dcm_pipeline(args)
        
        start = datetime.datetime.now()
        LOGGER.info(f'Start time of the extraction: {start}')
        extract(downloaded_file, ext_dir)
        edit = ['java', '-jar', 'dicom-edit6-1.0.8-SNAPSHOT-jar-with-dependencies.jar', '-s',
                args['anonymize_script'], '-i', ext_dir, '-o', out_dir]
        
        run_command_time = datetime.datetime.now()
        LOGGER.info(f'Start time of running dcmedit command time: {run_command_time}')
        run_command(edit)
        fdcms = glob.glob(out_dir+"/**", recursive=True)
        fdcms = [f for f in fdcms if os.path.isfile(f)]
        if len(fdcms) == 0: raise ValueError("No dicom files found")
        # here we have new rule for the dicom output file name
        # to avoid the name collision, we add the timestamp in suffix
        filename_out = os.path.splitext(os.path.basename(downloaded_file))[0] + '_edited'
        f_out = os.path.join(out_dir, filename_out)   

        # # for lock same here, since I know the new file is zip 
        # # so I will just lock the zip
        lock_resource(resource_key, "write")

        os.chdir(args['workdir'])
        shutil.make_archive(f_out, 'zip', args['workdir'], t+'o')
        f_out += '.zip'

        upload_zip_time = datetime.datetime.now()
        LOGGER.info(f'Start time of uploading zip command time: {upload_zip_time}')
        new_file_obj = upload_file(f_out, output_location, env, mc)
        LOGGER.info(f'output: {f_out}')

        #####################################################
        # TODO pass the geid here 
        try:
            # Get parent folder
            payload = {
                "label": "own",
                "start_label": "Folder",
                "end_label": "File",
                "end_params": {
                    "location": args["input_file"],
                }
            }
            response = requests.post(
                ConfigClass.NEO4J_SERVICE_V1 + "relations/query", json=payload)
            if response.json():
                parent_folder = response.json(
                )[0]["start_node"]
            else:
                parent_folder = None
        except Exception as e:
            LOGGER.debug('Error getting parent_folder: ' + str(e))
            raise e

        try:
            # Get input file node
            payload = {
                "location": args["input_file"]
            }
            response = requests.post(ConfigClass.NEO4J_SERVICE_V1 + "nodes/File/query", json=payload)
            input_node = response.json()[0]
            LOGGER.debug(f'Got parent node {input_node}')
        except Exception as e:
            error_msg = f"Error getting parent_node on dicom pipeline: {str(e)}"
            LOGGER.debug(error_msg)
            raise Exception(error_msg)

        # create metadata here
        project = {"code": args['project']}
        mf = MetaDataFactory(project, "auto_trigger", "greenroom",
            PROCESS_PIPELINE, PIPELINE_DESC, OPERATION_TYPE)

        # create new node for the ouput and linked with input node
        extra = {"parent_folder_geid": parent_folder["global_entity_id"]}
        new_node, _ = create_file_node(args['project'], input_node, "auto_trigger", 
            parent_folder.get("id"), parent_folder.get("display_path"), 
            new_file_obj, new_name=os.path.basename(output_location), extra_fields=extra)
        create_relation(input_node.get("id"), new_node.get("id"), label=PROCESS_PIPELINE)
        # print(new_node)
        
        # create the new node in atlas for lineage linking
        guid = mf.create_catalog_entity(new_node)
        # print(guid)

        # create linerage between new node and input file
        lin = mf.create_lineage_v3(
            input_node.get("global_entity_id"), 
            new_node.get("global_entity_id")
        )
        # print(lin)

        # es?
        mf.create_es_search_index(new_node, input_node, "File", guid)

        # create zip preview
        preview = parse_zip(f_out)
        save_preview(preview, new_node.get("global_entity_id"))

    except Exception as err:
        LOGGER.exception(err)
        sys.exit(err)  

    finally:
        # unlock it after upload
        unlock_resource(resource_key, "write")

    LOGGER.info('clear intermediate direcotries.')
    shutil.rmtree(ext_dir)
    shutil.rmtree(out_dir)
    LOGGER.info('All done.')

if __name__ == "__main__":
    args = parse_inputs()
    logname = args['log_file'] 
    log_path = os.path.dirname(logname)
    if log_path and not os.path.exists(log_path):
        os.makedirs(log_path)
    try:
        LOGGER = Logger(logname, True).logger
        LOGGER.info('Vault url: ' + os.getenv("VAULT_URL"))
        LOGGER.info("="*82)
        LOGGER.info(" Start dicom editing ...  ".center(82, '='))
        LOGGER.info("="*82)
    except LoggerException as e:
        print(e)
        sys.exit(-1)  

    env = args.get('environment', 'test')
    main(env)
