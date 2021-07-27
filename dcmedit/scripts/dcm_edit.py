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
import subprocess
import shutil
import json
import io
from minio_client import Minio_Client
from config import ConfigClass

from logger import Logger, LoggerException
import preprocess

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
    # parse from format minio://http://10.3.7.220/gr-generate/admin/generate_folder/ABC-1234_OIP.WH4UEecUNFLkLRAy3cbgQQHaEK.jpg
    config = ConfigClass(env)
    protocol = "https://" if config.MINIO_HTTPS else "http://"
    path = path.replace("minio://", "").replace(protocol, "").split("/")
    bucket = path[1]
    path = '/'.join(path[2:])
    return {"bucket": bucket, "path": path}

def download_file(minio_path, target_folder, env):
    file_data = parse_location(minio_path, env)
    mc = Minio_Client(env)
    response = mc.client.get_object(file_data["bucket"], file_data["path"])
    target_path = target_folder + "/" + file_data["path"].split("/")[-1]
    with open(target_path, 'wb') as f:
        f.write(response.data)
    return target_path

def upload_file(file_path, target_location, env):
    file_data = parse_location(target_location, env)
    filename = os.path.basename(file_data["path"])
    mc = Minio_Client(env)
    response = mc.client.fput_object(
        file_data["bucket"], 
        file_data["path"], 
        file_path, 
        metadata={"location": target_location.encode('utf-8')}
    )

def generate_output_location(input_file):
    filename = os.path.basename(input_file)
    output_filename = filename.split(".")[0] + "_edited.zip"
    return input_file.replace(filename, output_filename)
    
def main(env):   
    projects = [p.strip() for p in conf.get('projects', 'IDs').split(',')]
    if args['project'] not in projects and not args['anonymize_script']:
        raise ValueError(f"unknown project {args['project']}")
    t = datetime.datetime.now().strftime('%H%M%S') 
    t = os.path.basename(args['input_file']).split('.')[0] + t
    ext_dir = args['ext_dir'] = os.path.join(args['workdir'], t+'i')
    out_dir = os.path.join(args['workdir'], t+'o')
    output_location = generate_output_location(args["input_file"])
    try:
        os.makedirs(ext_dir) 
        LOGGER.info(f'extract dir: {ext_dir}')
        os.makedirs(out_dir)
        LOGGER.info(f'work dir: {out_dir}')
        downloaded_file = download_file(args["input_file"], ext_dir, env)
        if args['project'] == 'generate':
            preprocess.generate(args)
        extract(downloaded_file, ext_dir)
        edit = ['java', '-jar', 'dicom-edit6-1.0.8-SNAPSHOT-jar-with-dependencies.jar', '-s',
                args['anonymize_script'], '-i', ext_dir, '-o', out_dir]
        run_command(edit)
        fdcms = glob.glob(out_dir+"/**", recursive=True)
        fdcms = [f for f in fdcms if os.path.isfile(f)]
        if len(fdcms) == 0: raise ValueError("No dicom files found")
        f_out = os.path.splitext(os.path.basename(downloaded_file))[0] + '_edited'
        f_out = os.path.join(out_dir, f_out)   
        os.chdir(args['workdir'])
        shutil.make_archive(f_out, 'zip', args['workdir'], t+'o')
        f_out += '.zip'   
        upload_file(f_out, output_location, env)
        LOGGER.info(f'output: {f_out}')
    except Exception as err:
        LOGGER.exception(err)
        sys.exit(err)  
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
    conf = configparser.ConfigParser()
    conf.read('config.ini')
    try:
        LOGGER = Logger(logname, True).logger
        LOGGER.info("="*82)
        LOGGER.info(" Start dicom editing ...  ".center(82, '='))
        LOGGER.info("="*82)
    except LoggerException as e:
        print(e)
        sys.exit(-1)  

    env = args.get('environment', 'test')
    main(env)
