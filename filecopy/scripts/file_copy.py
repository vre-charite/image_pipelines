import os 
import subprocess
import argparse
import logging
import sys

#create logger for filecopy pipeline 
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_inputs():
    parser = argparse.ArgumentParser(
        description = __doc__,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('-i', '--input-path', help='Sepecify input file', 
        metavar='FILE/Folder', required=True)
    parser.add_argument('-o', '--output-path', help='Sepecify output file', 
        metavar='PATH', required=True)
    parser.add_argument('-l', '--log-path', help='Name of log file with full path',
        metavar='PATH', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def main():
    try:
        output_file = args['output_path']
        output_path = os.path.dirname(output_file)
        logger.debug(f'file path is: {output_path}')
        input_path = args['input_path']

        try:
            if not os.path.exists(output_path):
                os.makedirs(output_path)
                logger.debug(f'creating output directory: {output_path}')
        except FileExistsError as e:
            logger.info(e)
            pass 


        if os.path.isdir(input_path):
            logger.debug(f'starting to copy directory: {input_path}')
        else:
            logger.debug(f'starting to copy file: {input_path}')
        if os.path.isdir(input_path):
            input_path += "/"
        subprocess.call(['rsync', '-avz', '--min-size=1', input_path, output_file])
        logger.debug(f'Successfully copied file from {input_path} to {output_file}')
    except Exception as e:
        logger.exception(f'Failed to copy file from {input_path} to {output_file}\n {e}')


if __name__ == "__main__":
    args = parse_inputs()
    logpath = args['log_path']
    if not os.path.exists(logpath):
        os.makedirs(logpath)
    try:
        formatter = logging.Formatter('%(asctime)s - %(name)s - \
                              %(levelname)s - %(message)s')
        # File handler                      
        file_handler = logging.FileHandler(logpath+'/file_copy.log')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.DEBUG)
        # Standard Out Handler
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        stdout_handler.setLevel(logging.DEBUG)
        # Standard Err Handler
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setFormatter(formatter)
        stderr_handler.setLevel(logging.ERROR)
        # register handlers
        logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)
        logger.debug("="*82)
        logger.debug(" Start copy file...  ".center(82, '='))
        logger.debug("="*82)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)  

    main()
    

