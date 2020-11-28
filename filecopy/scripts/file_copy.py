import os 
import subprocess
import argparse
import logging
import sys

#create logger for filecopy pipeline 
logger = logging.getLogger(__name__)

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
        logger.info(f'file path is: {output_path}')
        input_path = args['input_path']

        if not os.path.exists(output_path):
            os.makedirs(output_path)
            logger.info(f'creating output directory: {output_path}')

        if os.path.isdir(input_path):
            logger.info(f'starting to copy directory: {input_path}')
        else:
            logger.info(f'starting to copy file: {input_path}')
        subprocess.call(['rsync', '-avz', '--min-size=1', input_path, output_file])
        logger.info(f'Successfully copied file from {input_path} to {output_file}')
    except Exception as e:
        logger.exception(f'Failed to copy file from {input_path} to {output_file}\n {e}')


if __name__ == "__main__":
    args = parse_inputs()
    logpath = args['log_path'] 
    try:
        formatter = logging.Formatter('%(asctime)s - %(name)s - \
                              %(levelname)s - %(message)s')
        file_handler = logging.FileHandler(logpath+'/file_copy.log')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.setLevel(logging.DEBUG)
        logger.info("="*82)
        logger.info(" Start copy file...  ".center(82, '='))
        logger.info("="*82)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)  

    main()
    

