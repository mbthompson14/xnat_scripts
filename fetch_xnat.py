#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx


##############################################################################################
#
## Fetch XNAT ##
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
# 7. Define files to copy to nyx
# 8. Copy relevant folders to nyx, preserving structure
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

# define parser
parser = argparse.ArgumentParser(
                    prog='fetch_xnat.py',
                    description='This script copies all RADCO dicom files on the XNAT server to the rawDicom folder on nyx3, \
                        that do not already exist on nyx3, preserving directory structure')

parser.add_argument('-d','--debug',
                    help='display detailed logging info for debugging',
                    action='store_true',
                    default=False)

args = parser.parse_args()

if args.debug:
    level = logging.DEBUG
else:
    level = logging.INFO

# make output directories if they don't exist
os.makedirs('LOGS/', exist_ok=True)  # can use pathlib if also need to make parent directories
os.makedirs('rawDicom/', exist_ok=True)


# define logger
logging.basicConfig(level=level, 
                    format='%(asctime)s %(levelname)s: %(message)s',
                    handlers=[
                        logging.FileHandler(f'LOGS/fetch_xnat_{datetime.now()}.log', mode='w'),
                        logging.StreamHandler(sys.stdout)
                    ])

# logging.info('info stuff')
# logging.debug('debug stuff')
# logging.warning('warning stuff')

print('Start logging...')

# define XNAT API parameters
HOST = 'https://xnatccn.semel.ucla.edu/'
AUTH = netrc().authenticators(HOST)
PROJECT = 'RADCO'
TARGET_PATH = 'rawDicom/'

if AUTH is None or AUTH[0] is None or AUTH[2] is None:
    raise Exception('.netrc not set up properly. Run this:\n\
                    echo "machine https://xnatccn.semel.ucla.edu/\n\
                    >     login USERNAME\n\
                    >     password PASSWORD" > ~/.netrc\n\
                    chmod 600 ~/.netrc')


logging.debug(f'HOST: {HOST}  PROJECT: {PROJECT}  TARGET_PATH: {TARGET_PATH}')

logging.info('Connecting to XNAT...')

# add try except here for connecting to xnat, or maybe it already raises informative exception?
with xnat.connect(HOST, AUTH[0], AUTH[2]) as session:
    if PROJECT in session.projects:
        radco = session.projects[PROJECT]
    logging.info(radco.subjects[0])


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