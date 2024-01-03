#!/nyx3/data/RAD22/Scripts/DataOrg/xnat_scripts/env/bin/python3

"""
Copy data from XNAT to NYX if data does not exist on NYX

matthew thompson
12.21.2023

"""

import os
import sys
import xnat
import timeit
import logging
import argparse
import pathlib
from pathlib import Path
import datetime
from util import logging_setup
from constants import HOST, PROJECT_ID, EXP_DIR, LOGS_DIR

def main():
    # --- CONSTANTS --- #
    DOWNLOAD_DIR = f'{EXP_DIR}/ImagingData/RawDICOM/XNAT_download'
    SUCCESS_DIR = f'{EXP_DIR}/ImagingData/RawDICOM/XNAT_download_list'

    success_file = f'{SUCCESS_DIR}/success_xnat_list_{datetime.datetime.now():%Y%m%d_%H%M}.txt'

    # define parser
    parser = argparse.ArgumentParser(
                        prog='xnat_download.py',
                        description='Downloads data on XNAT to local directory')

    # add debug option
    parser.add_argument('-d','--debug',
                        help='detailed logging info',
                        action='store_true',
                        default=False)
    
    # parse arguments from terminal
    args = parser.parse_args()

    # set logging level
    if args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    # initialize logging
    logging_setup(log_level=log_level, logs_dir=LOGS_DIR, name='download')
    
    with xnat.connect(server=HOST, default_timeout = 600, loglevel=logging.root.level) as session:
            if PROJECT_ID in session.projects:
                project = session.projects[PROJECT_ID]

                loop_subject(session=session, project=project, download_dir=DOWNLOAD_DIR, success_file=success_file)

            else:
                raise Exception("Project ID not found in XNAT")


def loop_subject(session: xnat.session.XNATSession, project: xnat.mixin.ProjectData, 
                       download_dir: str, success_file: str) -> None:
    
    project_uri = Path('/data','projects',project.id)

    # loop through all subjects in the xnat project
    for subject in project.subjects.values():
        
        subject_uri = Path(project_uri,'subjects',subject.label)  # xnat uri
        subject_path = Path(download_dir,subject.label)  # local directory
        subject_path_index = -1

        if not subject_path.is_dir() or not any(subject_path.iterdir()):
            logging.info(f'Directory {subject_path} does not exist or is empty. Attempting download.')

            downloaded = download(session=session, uri=subject_uri, path=subject_path, path_index=subject_path_index)

            if downloaded:
                write_success_list(subject=subject, success_file=success_file)
                continue  # move to next subject

            # if subject failed to download, attempt to download as sub-directories
            exp_success_list = []
            for exp in subject.experiments.values():
                exp_uri = Path(subject_uri,'experiments',exp.id)
                exp_path = Path(subject_path,exp.label)
                exp_path_index = -1

                downloaded = download(session=session, uri=exp_uri, path=exp_path, path_index=exp_path_index)
                exp_success_list.append(downloaded)

                if downloaded:
                    continue  # move to next exp
                
                scan_success_list = []
                for scan in exp.scans.values():
                    scan_uri = Path(exp_uri,'scans',scan.id)
                    scan_path = Path(exp_path,'-'.join((scan.id,scan._overwrites['type'].replace(' ','_'))))
                    scan_path_index = -2

                    downloaded = download(session=session, uri=scan_uri, path=scan_path, path_index=scan_path_index)
                    scan_success_list.append(downloaded)

                # if all scans were downloaded, write subject to success list
                if all(scan_success_list):
                    write_success_list(subject=subject, success_file=success_file)

            # if all experiments were downloaded, write subject to success list
            if all(exp_success_list):
                write_success_list(subject=subject, success_file=success_file)
        
        else:
            # directory alreay exists in the download directory
            logging.info(f'Path {subject_path} is a local directory and not empty. Moving on.')
            continue  # move on to next subject

def download(session: xnat.session.XNATSession, uri: pathlib.PosixPath, path: str, path_index: int | None = None) -> bool:

    start = timeit.default_timer()

    try:
        # attempt download
        session.create_object(str(uri)).download_dir(str(Path(*path.parts[:path_index])))
    except Exception as e:
        # download failed
        logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
        logging.info(f'Original exception: {e}')
        return False
    else:
        # download successful
        logging.info(f'Downloaded successfully: {path}')
        logging.info(f'Download elapsed time : {path} : {timeit.default_timer() - start}')
        return True

def write_success_list(subject: str, success_file: str) -> None:

    with open(success_file, 'a') as file:
        file.write(f'{datetime.datetime.now(datetime.timezone.utc).astimezone():%a %d %b %Y %H:%M:%S %Z} : {os.getlogin()} : successful download {subject.label}\n')

if __name__ == "__main__":
     main()
