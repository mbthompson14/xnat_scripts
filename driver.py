#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx

"""
Example driver for the XNATSync class

Matthew Thompson - 2023-11-03
mbthompson14@gmail.com

XNAT credentials must be stored in ~/.netrc in the following format:

    machine xnatccn.semel.ucla.edu
    login USERNAME
    password PASSWORD

The .netrc file can be created by running the following:

    echo "machine xnatccn.semel.ucla.edu
    >     login USERNAME
    >     password PASSWORD" > ~/.netrc
    chmod 600 ~/.netrc'
"""

from XNATSync import XNATSync
import argparse
import logging
import os

def main():
    HOST = 'https://xnatccn.semel.ucla.edu/'
    PROJECT_ID = 'sync_test'  # 'RADCO'
    LOCAL = 'test_data'  # path to nyx raw dicoms folder, I recommend creating a new, empty directory for this
    LOGS = 'logs'  # path to nyx logs folder

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
    
    # add option to check and download at the file level
    parser.add_argument('-f','--files',
                        help='Check and download at the level of individual files. This can take a long time. \
                            (Default is to check and download at the level of resources)',
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
        granularity = ''

    sync = XNATSync(logs=LOGS,log_level=level)
    sync.sync(host=HOST,project_id=PROJECT_ID,local_root=LOCAL,granularity=granularity)

if __name__ == '__main__':
    main()