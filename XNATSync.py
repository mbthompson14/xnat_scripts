
import os
import time
import xnat
import logging
import pathlib
import timeit
from pathlib import Path
from datetime import datetime

class XNATSync:
    def __init__(self, logs: str, log_level: int = logging.INFO) -> None:
        self._xnat = xnat
        self.logger = logging.basicConfig(level=log_level, 
                            format='%(asctime)s %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(f'{logs}/sync_xnat_{datetime.now()}.log', mode='w'),
                                #logging.StreamHandler(sys.stdout)
                            ])
    
    def sync(self, host: str, project_id: str, local_root: str, granularity: str = 'scans') -> None:

        logging.info('Starting sync...')

        with self._xnat.connect(server=host, default_timeout = 600, loglevel=logging.root.level) as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                
                #self._download_loop(session=session, project=project, local_root=local_root, granularity=granularity)

                # depth
                # subject = 1
                # exp = 2
                # scans = 4

                self._upload(session=session, project=project, local_root=local_root, depth=2)

            else:
                raise Exception("Project ID not found in XNAT")

    def _download_loop(self, session: xnat.session.XNATSession, project, local_root: str, granularity: str = 'scans') -> None:

        project_uri = Path('/data','projects',project.id)
        project_path = Path(local_root,project.id)
        project_path_index = 1

        downloaded = self._check_download(session=session,uri=project_uri,path=project_path,path_index=project_path_index)
        if downloaded:
            return

        for subject in project.subjects.values():
            subject_uri = Path(project_uri,'subjects',subject.label)
            subject_path = Path(project_path,subject.label)
            subject_path_index = 2

            downloaded = self._check_download(session=session,uri=subject_uri,path=subject_path,path_index=subject_path_index)
            if downloaded:
                continue

            for exp in subject.experiments.values():
                exp_uri = Path(subject_uri,'experiments',exp.id)
                exp_path = Path(subject_path,exp.label)
                exp_path_index = 3

                downloaded = self._check_download(session=session,uri=exp_uri,path=exp_path,path_index=exp_path_index)
                if downloaded:
                    continue

                for scan in exp.scans.values():
                    scan_uri = Path(exp_uri,'scans',scan.id)
                    scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'].replace(' ','_'))))
                    scan_path_index = 3

                    downloaded = self._check_download(session=session,uri=scan_uri,path=scan_path,path_index=scan_path_index)
                    if downloaded:
                        continue

                    for resource in scan.resources.values():
                        resource_uri = Path(scan_uri,'resources',resource.id)
                        resource_path = Path(scan_path,'resources',resource.label)
                        resource_path_index = 3

                        self._check_download(session=session,uri=resource_uri,path=resource_path,path_index=resource_path_index)

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

    def _check_download(self, session: xnat.session.XNATSession, uri: pathlib.PosixPath, path: pathlib.PosixPath, path_index: int) -> bool:
        
        logging.debug('In _check_download')
        logging.debug(f'uri: {uri}')
        logging.debug(f'path: {path}')

        if not path.is_dir() or not any(path.iterdir()):
            logging.debug('Path does not exist or is empty. Attempting download.')
            try:
                session.create_object(str(uri)).download_dir(str(Path(*path.parts[:path_index])))
            except Exception as e:
                logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
                logging.debug(f'Original exception: {e}')
                return False
            else:
                logging.debug(f'Downloaded successfully: {path}')
                return True
        else:
            logging.debug('Path is a local directory and not empty. Moving on.')
            return False
        
    def _check_download_file(self, session: xnat.session.XNATSession, uri: pathlib.PosixPath, path: pathlib.PosixPath) -> None:

        logging.debug('In _check_download_file')
        logging.debug(f'uri: {uri}')
        logging.debug(f'path: {path}')

        if not path.is_file():
            logging.debug('Path is not a local file. Attempting download.')
            try:
                session.create_object(str(uri)).download(str(path))
            except Exception as e:
                logging.warning(f'Problem downloading file: {path}')
                logging.debug(f'Original exception: {e}')
            else:
                logging.debug(f'Downloaded successfully: {path}')
        else:
            logging.debug('Path is a local file. Moving on.')
            return        

    def _upload(self, session: xnat.session.XNATSession, project, depth: int, local_root: str = '') -> None:
        """"""

        start = timeit.default_timer()
        project_path = str(Path(local_root,project.id))
        base_depth = project_path.rstrip(os.path.sep).count(os.path.sep)
        successful_upload = []

        for root, dirs, files in os.walk(top=project_path, topdown=True):

            # check prearchive for fun
            for s in session.prearchive.find(project=project.id):
                logging.debug(f'Prearchive: {s}, {s.status}')

            # what does this even do
            # session.services.refresh_catalog(resource=project)

            cur_depth = root.count(os.path.sep)
            # logging.info(f'depth: {depth}')
            # logging.info(f'base_depth: {base_depth}')
            # logging.info(f'root: {root}')
            # logging.info(f'cur_depth: {cur_depth}')
            if cur_depth - base_depth >= depth:
                logging.info('TOO DEEP, MOVING ON')
                continue

            for dir in dirs:
                full_path = str(Path(root,dir))
                parts = Path(full_path.replace(f'{local_root}/','')).parts
                xnat_dir = str(Path(*parts))
                project_id = parts[0]
                    
                if xnat_dir.count(os.path.sep) == 0:
                    continue

                if successful_upload and (parts[1] in successful_upload):
                    continue

                try:
                    logging.debug(f'Checking if directory {xnat_dir} exists in XNAT')

                    if len(parts) == 2:
                        assert session.projects[parts[0]].subjects[parts[1]]

                    elif len(parts) == 3:
                        try:
                            session.projects[parts[0]].subjects[parts[1]].experiments[parts[2]]
                        except:
                            exists_in_archive = False
                        else:
                            exists_in_archive = True

                        exists_in_prearchive = self._check_prearchive(session=session,project=project,subject=parts[1],exp=parts[2])

                        if not exists_in_archive and not exists_in_prearchive:
                            raise Exception()

                    elif len(parts) == 5:
                        assert session.projects[parts[0]].subjects[parts[1]].experiments[parts[2]].scans[parts[4].split(sep='-')[0]]

                    # if xnat_dir.count(os.path.sep) == 1:
                    #     assert session.projects[project_id].subjects[parts[1]]
                    # elif xnat_dir.count(os.path.sep) == 2:
                    #     assert session.projects[project_id].subjects[parts[1]].experiments[parts[2]]
                    # elif xnat_dir.count(os.path.sep) == 4:
                    #     assert session.projects[project_id].subjects[parts[1]].experiments[parts[2]].scans[parts[4].split(sep='-')[0]]
                    # could add assertions for more sub directories !!!

                    else:
                        logging.warning(f'Directory {xnat_dir} not a subject, experiment, or scan. Skipping upload.')
                        continue

                except Exception as e:
                    logging.info(f'Directory {xnat_dir} does not exist in XNAT, attempting to upload...')
                    logging.debug(f'Original exception: {e}')
                    try:
                        session.services.import_dir(directory=full_path, project=project_id, subject=parts[1])
                    except Exception as e:
                        logging.info(f'Directory {xnat_dir} failed to upload, will try uploading as subdirectories')
                        logging.debug(f'Original exception: {e}')
                    else:
                        logging.info(f'Directory {xnat_dir} successfully uploaded to XNAT')
                        logging.debug(f'UPLOAD ELAPSED TIME : {xnat_dir} : {timeit.default_timer() - start}')
                        successful_upload.append(parts[1])
                else:
                    logging.info(f'Directory {xnat_dir} exists in XNAT, moving on to next directory or subdirectory')


    def _check_prearchive(self, session: xnat.session.XNATSession, project, subject: str, exp: str) -> bool:
        status_list = ('ARCHIVE PENDING','ARCHIVING NOW','BUILD PENDING','BUILDING NOW','READY')

        for status in status_list:
            prearchive_search = session.prearchive.find(project=project,subject=subject,session=exp,status=status)
            logging.debug(f'PREARCHIVE SEARCH: {prearchive_search}')
            if prearchive_search:
                return True
            else:
                continue
        return False