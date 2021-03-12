import os 
import subprocess
import argparse
import logging
import sys
import shutil

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
    parser.add_argument('-t', '--trash-path', help='Trash folder path', 
        metavar='PATH', required=True)

    arguments = vars(parser.parse_args())
    return arguments


def main():
    try:
        output_file = args['output_path']
        output_path = os.path.dirname(output_file)
        logger.info(f'file path is: {output_path}')
        input_path = args['input_path']
        trash_path = args['trash_path']
        if not os.path.exists(trash_path):
            os.makedirs(trash_path)
            logger.info(f'creating trash folder: {trash_path}')
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            logger.info(f'creating output directory: {output_path}')
            
        if os.path.isdir(input_path):
            logger.info(f'starting to move directory: {input_path}')
        else:
            logger.info(f'starting to move file: {input_path}')
        shutil.move(input_path, output_file)
        logger.info(f'Successfully moved file from {input_path} to {output_file}')
    except Exception as e:
        logger.exception(f'Failed to move file from {input_path} to {output_file}\n {e}')


if __name__ == "__main__":
    args = parse_inputs()
    try:
        formatter = logging.Formatter('%(asctime)s - %(name)s - \
                              %(levelname)s - %(message)s')
        # File handler                      
        file_handler = logging.FileHandler('./file_move.log')
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
        
        logger.info("="*82)
        logger.info(" Start moving file...  ".center(82, '='))
        logger.info("="*82)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)  

    main()
    
