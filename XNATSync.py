
import os
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
                project_path = str(Path(local_root,project.id))
                
                #self._download_loop(session=session, project=project, local_root=local_root, granularity=granularity)

                self._upload(session=session, project=project, local_root=local_root, path=project_path, depth=2)

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
        


    # def _upload_loop(self, session: xnat.session.XNATSession, project: str, local_root: str) -> None:

    #     for path in self._walk(path=local_root, depth=2):

    #         parts = Path(path.replace(f'{local_root}/','')).parts
    #         project_id = parts[0]
    #         subject = parts[1]

    #         try:
    #             logging.debug(f'Checking if subject {subject} exists in XNAT')
    #             assert session.projects[project_id].subjects[subject]
    #         except Exception as e:
    #             logging.info(f'Subject {subject} does not exist in XNAT, attempting to upload...')
    #             logging.debug(f'Original exception: {e}')
    #             try:
    #                 session.services.import_dir(directory=path, project=project_id, subject=subject)
    #             except Exception as e:
    #                 logging.info(f'Subject {subject} failed to upload, will try uploading as subdirectories')
    #                 logging.debug(f'Original exception: {e}')
    #         else:
    #             logging.info(f'Subject {subject} exists in XNAT, moving on (either to subdirectory or next subject, depending on granularity)')



    # def _upload_loop(self, session: xnat.session.XNATSession, project: str, local_root: str) -> None:

    #     ## IS IT POSSIBLE TO UPLOAD THE ENTIRE PROJECT BUT SET OVERWRITE TO NONE????##
    #     ## I don't think so...

    #     logging.debug('In _upload_loop')

    #     for path in Path(local_root).rglob('*'):

    #         start = timeit.default_timer()
            
    #         if path.name == '.DS_Store':
    #             continue

    #         path_str = str(path)
    #         parts = Path(path_str.replace(f'{local_root}/','')).parts
    #         #dest = f'/archive/projects/{project_id}'

    #         if len(parts):
    #             project_id = parts[0]

    #             if project_id != project.id:
    #                 logging.warning(f'Local project directory name ({project_id}) does not match project ID in XNAT ({project.id})')

    #             #file_obj = connection.projects['project'].subjects['S'].experiments['EXP'].scans['T1'].resources['DICOM'].files[0]

    #             if len(parts) == 1:
    #                 continue
    #             elif len(parts) >= 2:
    #                 subject = parts[1]
    #                 try:
    #                     logging.debug(f'Checking if subject {subject} exists in XNAT')
    #                     assert session.projects[project_id].subjects[subject]
    #                 except Exception as e:
    #                     logging.info(f'Subject {subject} does not exist in XNAT, attempting to upload...')
    #                     logging.debug(f'Original exception: {e}')
    #                     try:
    #                         #import_path = str(Path(*Path(import_path).parts[:2]))
    #                         session.services.import_dir(directory=path, project=project_id, subject=subject)
    #                     except Exception as e:
    #                         logging.info(f'Subject {subject} failed to upload, will try uploading as subdirectories')
    #                         logging.debug(f'Original exception: {e}')
    #                         # if len(parts) >= 3:
    #                         #     exp = parts[2]
    #                         #     try:
    #                         #         logging.debug(f'Checking if experiment {exp} exists in XNAT')
    #                         #         assert session.projects[project_id].subjects[subject].experiments[exp]
    #                         #     except:
    #                         #         logging.info(f'Experiment {exp} does not exist in XNAT, attempting to upload...')
    #                         #         try:
    #                         #             #import_path = str(Path(*Path(import_path).parts[:3]))
    #                         #             session.services.import_dir(directory=path, destination=dest, project=project_id, subject=subject, experiment=exp)
    #                         #         except Exception as e:
    #                         #             logging.info(f'Experiment {exp} failed to upload, will try uploading as subdirectories')
    #                         #             logging.debug(f'Original exception: {e}')
    #                         #             if len(parts) >=4:
    #                         #                 scan = parts[3]
    #                         #                 try:
    #                         #                     logging.debug(f'Checking if scan {scan} exists in XNAT')
    #                         #                     assert session.projects[project_id].subjects[subject].experiments[exp].scan[scan]
    #                         #                 except:
    #                         #                     logging.info(f'Scan {scan} does not exist in XNAT, attempting to upload...')
    #                         #                     try:
    #                         #                         #import_path = str(Path(*Path(import_path).parts[:4]))
    #                         #                         session.services.import_dir(directory=path, destination=dest, project=project_id, subject=subject, experiment=exp)
    #                         #                     except Exception as e:
    #                         #                         logging.info(f'Scan {scan} failed to upload, will try uploading as subdirectories')
    #                         #                         logging.debug(f'Original exception: {e}')
    #                         #                     else:
    #                         #                         logging.info(f'Scan {scan} uploaded successfully')
    #                         #                         continue
    #                         #             else:
    #                         #                 continue
    #                         #         else:
    #                         #             logging.info(f'Experiment {exp} uploaded successfully')
    #                         #             continue
    #                         # else:
    #                         #     continue
    #                     else:
    #                         logging.info(f'Subject {subject} uploaded successfully')
    #                         continue
    #                 else:
    #                     logging.info(f'Subject {subject} already exisits in XNAT')
    #             else:
    #                 continue

    #         logging.debug(f'UPLOAD ELAPSED TIME : {path} : {timeit.default_timer() - start}')               


    def _walk(self, path: str, depth: int):
        """Recursively list files and directories up to a certain depth"""
        depth -= 1
        with os.scandir(path) as p:
            for entry in p:
                yield entry.path
                if entry.is_dir() and depth > 0:
                    yield from self._walk(entry.path, depth)


    def _upload(self, session: xnat.session.XNATSession, project: str, path: str, depth: int, local_root: str = '') -> None:
        """Recursively list files and directories up to a certain depth"""

        start = timeit.default_timer()

        depth -= 1
        #print('IN UPLOAD')

        with os.scandir(path) as p:
            for entry in p:
                #print('IN SCAN DIR')

                if entry.name == '.DS_Store':  # can remove once on UCLA server
                    continue

                parts = Path(entry.path.replace(f'{local_root}/','')).parts
                xnat_dir = str(Path(*parts))
                project_id = parts[0]

                if len(parts) == 1:
                    continue
                elif len(parts) >= 2:
                    #print('IN LEN PARTS 2')
                    try:
                        logging.debug(f'Checking if directory {xnat_dir} exists in XNAT')
                        if len(parts) == 2:
                            assert session.projects[project_id].subjects[parts[1]]
                        elif len(parts) == 3:
                            assert session.projects[project_id].subjects[parts[1]].experiments[parts[2]]
                        elif len(parts) == 5:
                            assert session.projects[project_id].subjects[parts[1]].experiments[parts[2]].scans[parts[4].split(sep='-')[0]]
                        # could add assertions for more sub directories !!!
                        else:
                            logging.warning(f'Directory {xnat_dir} not a subject, experiment, or scan. Skipping upload.')
                            continue
                    except Exception as e:
                        logging.info(f'Directory {xnat_dir} does not exist in XNAT, attempting to upload...')
                        logging.debug(f'Original exception: {e}')
                        try:
                            session.services.import_dir(directory=entry.path, project=project_id, subject=parts[1])
                        except Exception as e:
                            logging.info(f'Directory {xnat_dir} failed to upload, will try uploading as subdirectories')
                            logging.debug(f'Original exception: {e}')
                            if entry.is_dir() and depth > 0:
                                self._upload(self, project=project, path=entry.path, depth=depth)
                        else:
                            logging.info(f'Directory {xnat_dir} successfully uploaded to XNAT')
                            logging.debug(f'UPLOAD ELAPSED TIME : {xnat_dir} : {timeit.default_timer() - start}')
                            continue
                    else:
                        logging.info(f'Directory {xnat_dir} exists in XNAT, moving on (either to subdirectory or next subject, depending on granularity)')
                        if entry.is_dir() and depth > 0:
                          self._upload(self, project=project, path=entry.path, depth=depth)

                        














                #self._check_upload(session=session, path=str(path_rel), project_id=project_id, subject=subject, exp=exp)

                
                

                # self._check_upload(session=session, path=path_rel, project_id=project_id)

                # if len(path_rel.parts) < 1:
                #     subject = path_rel.parts[1]
                #     self._check_upload(session=session, path=path_rel, project_id=project_id, subject=subject)
                    
                #     if len(path_rel.parts) > 2:
                #         exp = path_rel.parts[2]
                #         self._check_upload(session=session, path=path_rel, project_id=project_id, subject=subject, exp=exp)

                #         if len(path_rel.parts) > 3:
                #             scan = path_rel.parts[3]
                #             self._check_upload(session=session, path=path_rel, project_id=project_id, subject=subject, exp=exp)

                #             if len(path_rel.parts) > 4:
                #                 resource = path_rel.parts[4]
                #                 self._check_upload(session=session, path=path_rel, project_id=project_id, subject=subject, exp=exp)

                                # if len(path_rel.parts) > 5:
                                #     file = path_rel.parts[5]
                                #     self._check_upload(session=session, path=path_rel, project_id=project_id, subject=subject, exp=exp)

    # def _check_upload(self, session, path: pathlib.PosixPath, project_id: str | None = None, subject: str | None = None, 
    #                   exp: str | None = None):
    #     pass
    #     # does it exist on xnat?
        
    #     # if any(path.iterdir()):
    #     #     session.import_dir(directory=path)

    # def _check_exist_xnat(self, session: xnat.session.XNATSession, parts: tuple):
    #     project_id = parts[0]
    #     try:
    #         obj = session.projects[project_id]
    #     except:
    #         return False
    #     else:
    #         return True
    #     if len(parts) == 1:
    #         project_id = parts[0]
    #         try:
    #             obj = session.projects[project_id]
    #         except:
    #             return False
    #         else:
    #             return True

