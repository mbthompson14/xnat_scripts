#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx

"""
XNATSync class definition

Matthew Thompson - 2023-11-03
mbthompson14@gmail.com

https://xnat.readthedocs.io/en/latest/index.html


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

import os
import xnat
import timeit
import logging
import pathlib
from pathlib import Path
from datetime import datetime

class XNATSync:
    """
    Collection of functions that use the XNATpy library to synchronize project directories on XNAT 
    with local project directories
    """
    def __init__(self, logs: str, log_level: int = logging.INFO) -> None:
        """
        Initialize class XNATSync

        :logs: string to identify location to store logs, e.g. 'logs'
        :log_level: level at which to log, e.g. logging.DEBUG, logging.INFO
        :returns: None
        """
        self._xnat = xnat
        self.logger = logging.basicConfig(level=log_level, 
                            format='%(asctime)s %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(f'{logs}/sync_xnat_{datetime.now()}.log', mode='w'),
                            ])
    
    def sync(self, host: str, project_id: str, local_root: str, granularity: str = '') -> None:
        """
        Synchronize local project directory with project directory hosted on XNAT server.

        NOTE: to establish connection with XNAT server, XNAT login credentials must be stored in ~/.netrc
        in the following format:

            machine xnatccn.semel.ucla.edu
            login USERNAME
            password PASSWORD

        The .netrc file can be created by running the following:

            echo "machine xnatccn.semel.ucla.edu
            >     login USERNAME
            >     password PASSWORD" > ~/.netrc
            chmod 600 ~/.netrc'

        :host: uri of XNAT server, e.g. 'https://xnatccn.semel.ucla.edu/'
        :project_id: string of project ID in XNAT
        :local_root: local directory containing the project directory
        :granularity: level at which to download directories, default is resources, can be set to 'files'
        :returns: None
        """
        logging.info('BEGIN SYNC')

        with self._xnat.connect(server=host, default_timeout = 600, loglevel=logging.root.level) as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                
                self._download_loop(session=session, project=project, local_root=local_root, granularity=granularity)

                self._upload(session=session, project=project, local_root=local_root, depth=4)

            else:
                raise Exception("Project ID not found in XNAT")

    def _download_loop(self, session: xnat.session.XNATSession, project: xnat.mixin.ProjectData, 
                       local_root: str, granularity: str = '') -> None:
        """
        Traverses the project directory on the XNAT server

        May be possible to do this recursively

        :session: XNATpy connection object
        :project: XNATpy project object
        :local_root: local directory containing the project directory
        :granularity: level at which to download directories, default is resources, can be set to 'files'
        :returns: None
        """

        logging.info('BEGIN DOWNLOAD')

        project_uri = Path('/data','projects',project.id)
        project_path = Path(local_root,project.id)
        project_path_index = 1

        downloaded = self._check_download(session=session,uri=project_uri,path=project_path,
                                          path_index=project_path_index)
        if downloaded:
            return

        for subject in project.subjects.values():
            subject_uri = Path(project_uri,'subjects',subject.label)
            subject_path = Path(project_path,subject.label)
            subject_path_index = 2

            downloaded = self._check_download(session=session,uri=subject_uri,path=subject_path,
                                              path_index=subject_path_index)
            if downloaded:
                continue

            for exp in subject.experiments.values():
                exp_uri = Path(subject_uri,'experiments',exp.id)
                exp_path = Path(subject_path,exp.label)
                exp_path_index = 3

                downloaded = self._check_download(session=session,uri=exp_uri,path=exp_path,
                                                  path_index=exp_path_index)
                if downloaded:
                    continue

                for scan in exp.scans.values():
                    scan_uri = Path(exp_uri,'scans',scan.id)
                    scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'].replace(' ','_'))))
                    scan_path_index = 3

                    downloaded = self._check_download(session=session,uri=scan_uri,path=scan_path,
                                                      path_index=scan_path_index)
                    if downloaded:
                        continue

                    for resource in scan.resources.values():
                        resource_uri = Path(scan_uri,'resources',resource.id)
                        resource_path = Path(scan_path,'resources',resource.label)
                        resource_path_index = 3

                        self._check_download(session=session,uri=resource_uri,path=resource_path,
                                             path_index=resource_path_index)

                        if granularity == 'files':
                            for file in resource.files.values():
                                file_uri = Path(resource_uri,'files',file.id)

                                if file.name.startswith(exp.id):
                                    file_name = file.name.replace(exp.id,exp.label)
                                    file_path = Path(resource_path,'files',file_name)
                                else:
                                    file_path = Path(resource_path,'files',file.name)

                                self._check_download_file(session=session,uri=file_uri,path=file_path)
                        else:
                            continue

    def _check_download(self, session: xnat.session.XNATSession, uri: pathlib.PosixPath, 
                        path: pathlib.PosixPath, path_index: int) -> bool:
        """
        Checks if an object on the XNAT server exists in the local project directory. 
        If the directory exists, does nothing. If the directory does not exist, attempts to download.

        :session: XNATpy connection object
        :uri: pathlib.Path uri of the object on the XNAT server
        :path: pathlib.Path path to the local directory of the object
        :path_index: integer specifying local directory level
        :returns: boolean indicating whether a directory was downloaded
        """
        
        start = timeit.default_timer()

        logging.debug('In _check_download')
        logging.debug(f'uri: {uri}')
        logging.debug(f'path: {path}')

        if not path.is_dir() or not any(path.iterdir()):
            logging.info(f'Path {path} does not exist or is empty. Attempting download.')
            try:
                session.create_object(str(uri)).download_dir(str(Path(*path.parts[:path_index])))
            except Exception as e:
                logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
                logging.info(f'Original exception: {e}')
                return False
            else:
                logging.info(f'Downloaded successfully: {path}')
                logging.debug(f'DOWNLOAD ELAPSED TIME : {path} : {timeit.default_timer() - start}')
                return True
        else:
            logging.info(f'Path {path} is a local directory and not empty. Moving on.')
            return False
        
    def _check_download_file(self, session: xnat.session.XNATSession, uri: pathlib.PosixPath, 
                             path: pathlib.PosixPath) -> None:
        """
        Checks if a file object on the XNAT server exists in the local project directory. 
        If the file exists, does nothing. If the file does not exist, attempts to download.

        :session: XNATpy connection object
        :uri: pathlib.Path uri of the object on the XNAT server
        :path: pathlib.Path path to the local directory of the object
        :returns: None
        """

        logging.debug('In _check_download_file')
        logging.debug(f'download uri: {uri}')
        logging.debug(f'download path: {path}')

        if not path.is_file():
            logging.info('Path is not a local file. Attempting download.')
            try:
                session.create_object(str(uri)).download(str(path))
            except Exception as e:
                logging.warning(f'Problem downloading file: {path}')
                logging.info(f'Original exception: {e}')
            else:
                logging.info(f'Downloaded successfully: {path}')
        else:
            logging.info('Path is a local file. Moving on.')
            return        

    def _upload(self, session: xnat.session.XNATSession, project: xnat.mixin.ProjectData, 
                depth: int, local_root: str = '') -> None:
        """
        Traverses local project directory, uploading folders to XNAT if they do not exist in XNAT

        Can set the directory level (depth) at which to check 1 (subjects), 2 (experiments), 4 (scans).
        (Possible to add deeper levels in future version, e.g. resources, files)

        TODO: add resource and file levels

        E.g. if depth is set to 2, it will check and upload any subjects or experiments that exist in 
        the local directory but not in XNAT.

        :session: XNATpy connection object
        :project: XNATpy project object
        :depth: level at which to scan and upload directories
        :local_root: the local root folder in which the project folder is contained
        :returns: None
        """

        logging.info('BEGIN UPLOAD')

        start = timeit.default_timer()
        project_path = str(Path(local_root,project.id))
        base_depth = project_path.rstrip(os.path.sep).count(os.path.sep)
        successful_upload = []

        for root, dirs, files in os.walk(top=project_path, topdown=True):

            cur_depth = root.count(os.path.sep)
            if cur_depth - base_depth >= depth:
                continue

            for dir in dirs:
                full_path = str(Path(root,dir))
                parts = Path(full_path.replace(f'{local_root}/','')).parts
                xnat_dir = str(Path(*parts))
                    
                if len(parts) == 1:
                    continue

                try:
                    logging.info(f'Checking if directory {xnat_dir} exists in XNAT')
                    if len(parts) == 2:
                        assert session.projects[parts[0]].subjects[parts[1]]

                    elif len(parts) == 3:
                        if parts[1] in successful_upload:
                            logging.info(f'Directory {xnat_dir} already uploaded via parent directory')
                            continue
                        assert session.projects[parts[0]].subjects[parts[1]].experiments[parts[2]]

                    elif len(parts) == 5:
                        if (parts[1]) in successful_upload or (parts[1],parts[2]) in successful_upload:
                            logging.info(f'Directory {xnat_dir} already uploaded via parent directory')
                            continue
                        assert session.projects[parts[0]].subjects[parts[1]].experiments[parts[2]].scans[parts[4].split(sep='-')[0]]

                    else:
                        logging.info(f'Directory {xnat_dir} not a subject, experiment, or scan. Skipping upload.')
                        continue

                except Exception as e:
                    logging.info(f'Directory {xnat_dir} does not exist in XNAT, attempting to upload...')
                    logging.debug(f'Original exception: {e}')
                    try:
                        session.services.import_dir(directory=full_path, overwrite='append', project=parts[0], subject=parts[1])
                    except Exception as e:
                        logging.warning(f'Directory {xnat_dir} failed to upload, will try uploading as subdirectories')
                        logging.info(f'Original exception: {e}')
                    else:
                        logging.info(f'Directory {xnat_dir} successfully uploaded to XNAT')
                        logging.debug(f'UPLOAD ELAPSED TIME : {xnat_dir} : {timeit.default_timer() - start}')
                        if len(parts) == 2:
                            successful_upload.append((parts[1]))
                        elif len(parts) == 3:
                            successful_upload.append((parts[1],parts[2]))
                else:
                    logging.info(f'Directory {xnat_dir} exists in XNAT, moving on to next directory or subdirectory')