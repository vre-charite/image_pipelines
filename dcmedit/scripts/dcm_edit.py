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

from logger import Logger, LoggerException
import preprocess

def parse_inputs():
    parser = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-c', '--config-file', help='Secify config file', 
            metavar='FILE', required=False)
    parser.add_argument('-i', '--input-file', help='Input zip file with full path', 
            metavar='FILE', required=True)
    parser.add_argument('-o', '--output-dir', help='Folder for output file',
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
    arguments = vars(parser.parse_args())
    if not arguments['log_file']: 
        arguments['log_file'] = os.path.join(arguments['output_dir'], "dcm_edit.log")
    if arguments['use_default_anonymization']:
        vars['anonymize_script'] = 'dicomedit_scripts/anonymization.des'
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
    
def main():   
    projects = [p.strip() for p in conf.get('projects', 'IDs').split(',')]
    if args['project'] not in projects and not args['anonymize_script']:
        raise ValueError(f"unknown project {args['project']}")
    t = datetime.datetime.now().strftime('%H%M%S') 
    t = os.path.basename(args['input_file']).split('.')[0] + t
    ext_dir = args['ext_dir'] = os.path.join(args['workdir'], t+'i')
    out_dir = os.path.join(args['workdir'], t+'o')

    try:
        os.makedirs(ext_dir) 
        LOGGER.info(f'extract dir: {ext_dir}')
        os.makedirs(out_dir)
        LOGGER.info(f'work dir: {out_dir}')
        if args['project'] == 'GENERATE':
            preprocess.generate(args)
        extract(args['input_file'], ext_dir)
        edit = ['java', '-jar', 'dicom-edit6-1.0.8-SNAPSHOT-jar-with-dependencies.jar', '-s',
                args['anonymize_script'], '-i', ext_dir, '-o', out_dir]
        run_command(edit)
        fdcms = glob.glob(out_dir+"/**", recursive=True)
        fdcms = [f for f in fdcms if os.path.isfile(f)]
        if len(fdcms) == 0: raise ValueError("No dicom files found")
        f_out = os.path.splitext(os.path.basename(args['input_file']))[0] + '_edited'
        f_out = os.path.join(args['output_dir'], f_out)   
        os.chdir(args['workdir'])
        shutil.make_archive(f_out, 'zip', args['workdir'], t+'o')
        f_out += '.zip'   
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

    main()