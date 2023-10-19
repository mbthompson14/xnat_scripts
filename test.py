from XNATSync import XNATSync
import argparse
import logging
import os

def main():
    HOST = 'https://xnatccn.semel.ucla.edu/'
    PROJECT_ID = 'RADCO'
    LOCAL = 'RawDICOM'
    LOGS = 'logs'

    # make directories if they don't exist
    os.makedirs(LOGS+'/', exist_ok=True) # can use pathlib if also need to make parent directories
    os.makedirs(LOCAL+'/', exist_ok=True)

    # define parser
    parser = argparse.ArgumentParser(
                        prog='sync_xnat.py',
                        description='Synchronizes a project directory on XNAT with a local target directory.')

    # add debug option
    parser.add_argument('-d','--debug',
                        help='display detailed logging info for debugging',
                        action='store_true',
                        default=False)
    
    parser.add_argument('-f','--files',
                        help='Check and download at the level of individual files. This takes a long time. (Default is to check and download at the level of scans)',
                        action='store_true',
                        default=False)

    # parse arguments from terminal
    args = parser.parse_args()

    # set logging level
    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    # set granularity
    if args.files:
        granularity = 'files'
    else:
        granularity = 'scans'

    sync = XNATSync(logs=LOGS,log_level=level)
    sync.sync(host=HOST,project_id=PROJECT_ID,local_root=LOCAL,granularity=granularity)

if __name__ == '__main__':
    main()