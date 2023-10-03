#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx


##############################################################################################
#
## Sync XNAT ##
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
#
# XNAT credentials should be stored in ~/.netrc in the following format:
#
# machine xnatccn.semel.ucla.edu
# login USERNAME
# password PASSWORD
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
import json
from datetime import datetime
import xnat
from netrc import netrc
from urllib import parse

# pathways
LOGS_PATH = 'LOGS/'  # can use pathlib if also need to make parent directories
NYX_DICOM = 'RawDICOM/'
# XNAT API parameters
HOST = 'https://xnatccn.semel.ucla.edu/'
PROJECT_ID = 'RADCO'

#TODO: package installer function

# setup logger
def init_logger(LOGS_PATH: str, level: int) -> logging.Logger:
    logger = logging.basicConfig(level=level, 
                        format='%(asctime)s %(levelname)s: %(message)s',
                        handlers=[
                            logging.FileHandler(f'{LOGS_PATH}fetch_xnat_{datetime.now()}.log', mode='w'),
                            #logging.StreamHandler(sys.stdout)
                        ])
    return logger


#logging.debug(f'HOST: {HOST}  PROJECT: {PROJECT_ID}  TARGET_PATH: {NYX_DICOM}  USER: {AUTH[0]}')

def get_xnat_json(HOST: str, PROJECT_ID: str, logger: logging.Logger, level:int):
    logging.info('Connecting to XNAT...')

    # add try except here for connecting to xnat, or maybe it already raises informative exception?
    with xnat.connect(HOST, loglevel=level,logger=logger) as session:
        if PROJECT_ID in session.projects:
            radco = session.projects[PROJECT_ID]
            #logging.info(radco.subjects[0])

            # find a way to get json of all files for all subjects in radco
            # file_obj = connection.projects['project'].subjects['S'].experiments['EXP'].scans['T1'].resources['DICOM'].files[0]

            for subject in radco.subjects.values():
                for session in subject.experiments.values():
                    for scan in session.scans.values():
                        for resource in scan.resources.values():
                            for file in resource.files.values():
                                print(file)


            # # Check if there are actually file to be found
            # self equals experiment here
            # file_list = self.xnat_session.get_json(self.fulluri + '/scans/ALL/files')
            # if len(file_list['ResultSet']['Result']) == 0:

            # SubjectData = session.classes.SubjectData
            # print(SubjectData.query().filter(SubjectData.project == PROJECT_ID).tabulate_dict())


            # for subject in radco.subjects.values():
            #    for exp in subject.experiments.values():
            #         print(session.get_json(f'/data/projects/{PROJECT_ID}/subjects/{subject}/experiments'))
                #    for scan in exp.scans.values():
                #        print(json.dumps(scan))
            #print(session.get_json(f'/data/projects/{PROJECT_ID}/subjects'))
        else:
            raise Exception('Project ID not in XNAT. Check PROJECT_ID variable in sync_xnat.py')

# def get_xnat_json():
#     logging.info('Connecting to XNAT...')

#     # add try except here for connecting to xnat, or maybe it already raises informative exception?
#     with xnat.connect(HOST, AUTH[0], AUTH[2]) as session:
#         if PROJECT_ID in session.projects:
#             radco = session.projects[PROJECT_ID]
#             #logging.info(radco.subjects[0])

#             # find a way to get json of all files for all subjects in radco

#             for subject in radco.subjects.values():
#                 print(subject.label)
#             #print(session.get_json(f'/data/projects/{PROJECT_ID}'))
#         else:
#             raise Exception('Project ID not in XNAT. Check PROJECT_ID variable in sync_xnat.py')



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


if __name__ == "__main__":
    # make directories if they don't exist
    os.makedirs(LOGS_PATH, exist_ok=True)
    os.makedirs(NYX_DICOM, exist_ok=True)

    # make sure .netrc is set up properly
    netrc_message = '.netrc not set up properly. Run this:\n\
                            echo "machine xnatccn.semel.ucla.edu\n\
                            >     login USERNAME\n\
                            >     password PASSWORD" > ~/.netrc\n\
                            chmod 600 ~/.netrc'
    try:
        AUTH = netrc().authenticators(parse.urlparse(HOST).netloc)
    except Exception as e:
        raise Exception(f'{netrc_message}\n\n Original exception: {e}')

    if AUTH is None or AUTH[0] is None or AUTH[2] is None:
        raise Exception(netrc_message)
    
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

    # set logging level
    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger = init_logger(LOGS_PATH, level)
    get_xnat_json(HOST,PROJECT_ID,logger,level)

# Might have to write my own download_dir function bc the built in function doesn't always recognize the zipped files as zipped files



# if __name__ == "__main__":
#     main()