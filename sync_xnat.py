#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx


##############################################################################################
#
## Sync XNAT##
#
# This script copies all RADCO dicom files on the XNAT server to the rawDicom folder on nyx3,
# that do not already exist on nyx3, preserving directory structure
# 
# Matthew Thompson - 9/28/23
# mbthompson14@gmail.com
#
# https://xnat.readthedocs.io/en/latest/index.html
#
###############################################################################################


# STEPS
#
# 1. Setup logger
# 2. Setup argument parser
#   a. What arguments? debug mode
# 3. Define API parameters: URL, username, password
# 4. Define target directory on nyx - RADCO rawDicom
# 5. Query XNAT to see what subjects/files exist
#   a. log files found
# 6. Compare XNAT files to files already on nyx
# 7. Determine which files need to be copied from xnat to nyx and vice versa
# 8. Copy relevant folders, preserving structure
#   a. log files copied
# 9. Check to make sure files copied properly
# 10. Generate list of existing/missing files for relevant subjects?
#


# import libraries
import os
import sys
import argparse
import logging
from datetime import datetime
import xnat
from netrc import netrc

## CONSTANTS ##
# pathways
LOGS = 'LOGS/'  # can use pathlib if also need to make parent directories
NYX_DICOM = 'RawDICOM/'

# make directories if they don't exist
os.makedirs(LOGS, exist_ok=True)
os.makedirs(NYX_DICOM, exist_ok=True)

# XNAT API parameters
HOST = 'https://xnatccn.semel.ucla.edu/'
PROJECT_ID = 'RADCO'

# make sure .netrc is set up properly
NETRC_INST = '.netrc not set up properly. Run this:\n\
                        echo "machine https://xnatccn.semel.ucla.edu/\n\
                        >     login USERNAME\n\
                        >     password PASSWORD" > ~/.netrc\n\
                        chmod 600 ~/.netrc'
try:
    AUTH = netrc().authenticators(HOST)
except:
    raise Exception(NETRC_INST)

if AUTH is None or AUTH[0] is None or AUTH[2] is None:
        raise Exception(NETRC_INST)


## LOGGING ##
# define parser
parser = argparse.ArgumentParser(
                    prog='sync_xnat.py',
                    description='This script copies all RADCO dicom files on the XNAT server to the rawDicom folder on nyx3, \
                        that do not already exist on nyx3, preserving directory structure')

# add debug option
parser.add_argument('-d','--debug',
                    help='display detailed logging info for debugging',
                    action='store_true',
                    default=False)

# parse arguments from terminal
args = parser.parse_args()

if args.debug:
    level = logging.DEBUG
else:
    level = logging.INFO

# define logger
logger = logging.basicConfig(level=level, 
                    format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler(f'{LOGS}fetch_xnat_{datetime.now()}.log', mode='w'),
                        logging.StreamHandler(sys.stdout)
                    ])
print('Start logging...')


logging.debug(f'HOST: {HOST}  PROJECT: {PROJECT_ID}  TARGET_PATH: {NYX_DICOM}  USER: {AUTH[0]}')


def get_xnat_json():
    logging.info('Connecting to XNAT...')

    # add try except here for connecting to xnat, or maybe it already raises informative exception?
    with xnat.connect(HOST, AUTH[0], AUTH[2]) as session:
        if PROJECT_ID in session.projects:
            radco = session.projects[PROJECT_ID]
            #logging.info(radco.subjects[0])

            # find a way to get json of all files for all subjects in radco

            for subject in radco.subjects.values():
                print(subject.label)
            #print(session.get_json(f'/data/projects/{PROJECT_ID}'))
        else:
            raise Exception('Project ID not in XNAT')



# get list of subjects in nyx3 rawDicom

# get list of subjects on xnat

# select subjects not in nyx3

# do this file by file?

# <SubjectData RAD22_UCLA_ZP20230130_121_01 (XNAT_S04886)>


# DOWNLOAD
# subject = experiment.subject

# subject.download_dir('/home/hachterberg/temp/')


# Downloading http://120.0.0.1/xnat/data/experiments/demo_E00091/scans/ALL/files?format=zip:
# 23736 kb
# Downloaded image session to /home/hachterberg/temp/ANONYMIZ3
# Downloaded subject to /home/hachterberg/temp/ANONYMIZ3


# def main():
#     setup_logging()
#     get_xnat_names()
    
    # get subject, file names from xnat
    # get subject, file names from nyx
    # compare
    # copy to xnat
    # copy to nyx


def main():
    get_xnat_json()

if __name__ == "__main__":
    main()
