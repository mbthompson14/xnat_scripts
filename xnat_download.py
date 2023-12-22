#!/Users/matthew/anaconda3/bin/python

"""
Copy data from XNAT to NYX if data does not exist on NYX

matthew thompson
12.21.2023

"""

import sys
import xnat
import timeit
import logging
import pathlib
from pathlib import Path
from util import logging_setup
from constants import HOST, PROJECT_ID, EXP_DIR, LOGS_DIR

def main():
    # --- CONSTANTS --- #
    DOWNLOAD_DIR = f'{EXP_DIR}/ImagingData/RawDICOM/XNAT_download'
    SUCCESS_DIR = f'{EXP_DIR}/ImagingData/RawDICOM/XNAT_download_list'

    logging_setup(log_level=logging.DEBUG, logs_dir=LOGS_DIR, name='download')  # get log level from arg
    
    with xnat.connect(server=HOST, default_timeout = 600, loglevel=logging.root.level) as session:
            if PROJECT_ID in session.projects:
                project = session.projects[PROJECT_ID]

                download_loop(session=session, project=project, download_dir=DOWNLOAD_DIR)

            else:
                raise Exception("Project ID not found in XNAT")

def download_loop(session: xnat.session.XNATSession, project: xnat.mixin.ProjectData, 
                       download_dir: str) -> None:
        """
        Traverses the project directory on the XNAT server. Calls check_download for each subject & scan.

        :session: XNATpy connection object
        :project: XNATpy project object
        :local_root: local directory containing the project directory
        :returns: None
        """

        logging.info('BEGIN DOWNLOAD')

        project_uri = Path('/data','projects',project.id)

        for subject in project.subjects.values():
            subject_uri = Path(project_uri,'subjects',subject.label)
            subject_path = Path(download_dir,subject.label)
            #subject_path_index = 2

            #downloaded = 
            check_download(session=session,uri=subject_uri,path=subject_path,download_dir=download_dir)

            # if downloaded:
            #     continue

            # maybe I don't bother with scans??

            # for scan in subject.scans.values():
            #     pass

                #TODO: get scan uri
                # somehow check if the scan exists in nyx
                #  
            #     scan_uri = Path(exp_uri,'scans',scan.id)
            #     scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'].replace(' ','_'))))
            #     scan_path_index = 3

            #     downloaded = check_download(session=session,uri=scan_uri,path=scan_path,
            #                                         path_index=scan_path_index)
            #     if downloaded:
            #         continue

def check_download(session: xnat.session.XNATSession, uri: pathlib.PosixPath, 
                    path: str, download_dir: str) -> bool:
    """
    Checks if an object on the XNAT server exists in the local project directory. 
    If the directory exists, does nothing. If the directory does not exist, attempts to download.

    :session: XNATpy connection object
    :uri: pathlib.Path uri of the object on the XNAT server
    :path: pathlib.Path path to the local directory of the object

    :returns: boolean indicating whether a directory was downloaded
    """
    
    start = timeit.default_timer()

    logging.debug('In check_download')
    logging.debug(f'uri: {uri}')
    logging.debug(f'local path: {path}')

    if not path.is_dir() or not any(path.iterdir()):
        logging.info(f'Directory {path} does not exist or is empty. Attempting download.')
        try:
            session.create_object(str(uri)).download_dir(download_dir)  # str(Path(*path.parts[:path_index])))
        except Exception as e:
            logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
            logging.info(f'Original exception: {e}')
            return False
        else:
            logging.info(f'Downloaded successfully: {path}')
            logging.debug(f'Download elapsed time : {path} : {timeit.default_timer() - start}')
            return True
    else:
        logging.info(f'Path {path} is a local directory and not empty. Moving on.')
        return False
    

#TODO: make success & fail file


if __name__ == "__main__":
     main()
