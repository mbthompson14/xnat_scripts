


import xnat
import logging
import pathlib
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

        with self._xnat.connect(host, loglevel=logging.root.level) as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                
                self._download_loop(session=session, project=project, local_root=local_root, granularity=granularity)

                # self._upload

            else:
                raise Exception("Project ID not found in XNAT")
            

    # def _download_loop(self, session: xnat.session.XNATSession, project, local_root: str) -> None:

    #     project_uri = Path('/data','projects',project.id)
    #     project_path = Path(local_root,'data','projects',project.id)

    #     self._check_download(session=session,uri=project_uri,path=project_path)

    #     for subject in project.subjects.values():

    #         subject_uri = Path(project_uri,'subjects',subject.label)
    #         subject_path = Path(project_path,'subjects',subject.label)

    #         self._check_download(session=session,uri=subject_uri,path=subject_path)

    #         for exp in subject.experiments.values():
    #             exp_uri = Path(subject_uri,'experiments',exp.id)
    #             exp_path = Path(subject_path,'experiments',exp.label)

    #             self._check_download(session=session,uri=exp_uri,path=exp_path)

    #             for scan in exp.scans.values():
    #                 scan_uri = Path(exp_uri,'scans',scan.id)
    #                 scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'])))

    #                 self._check_download(session=session,uri=scan_uri,path=scan_path)

    #                 for resource in scan.resources.values():
    #                     resource_uri = Path(scan_uri,'resources',resource.id)
    #                     resource_path = Path(scan_path,'resources',resource.label)

    #                     self._check_download(session=session,uri=resource_uri,path=resource_path)

    #                     for file in resource.files:
    #                         file_uri = Path(resource_uri,'files',file)
    #                         file_path = Path(resource_path,'files',file)

    #                         self._check_download_file(session=session,uri=file_uri,path=file_path)


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








    ###################
    # Failed Attempts #
    ###################

    # def _download_recur(self, session: xnat.session.XNATSession, current_dir, local_root: str, subject_label: str = None) -> None:

    #     # get URI
    #     uri = current_dir.uri

    #     # get local path
    #     if isinstance(current_dir, session.classes.SubjectData):
    #         subject_label = current_dir.label
    #         path = Path(local_root, *Path(uri[1:]).parts[:-1], subject_label)
    #         logging.warning(f'SUBJECT PATH: {path}')
    #     elif isinstance(current_dir, session.classes.MrSessionData):
    #         temp_path = [local_root, *Path(uri[1:]).parts[:-1], current_dir.label]
    #         temp_path[5] = subject_label
    #         path = Path(*temp_path)
    #         logging.warning(f'EXP PATH: {path}')
    #     elif isinstance(current_dir, session.classes.MrScanData):
    #         temp_path = [local_root, *Path(uri[1:]).parts[:-1], '-'.join((current_dir.id,current_dir._overwrites['type']))]
    #         temp_path[5] = subject_label
    #         temp_path[7] = session.experiments[current_dir.image_session_id].label
    #         path = Path(*temp_path)
    #         logging.warning(f'SCAN PATH: {path}')
    #     elif isinstance(current_dir, session.classes.ResourceCatalog):
    #         temp_path = [local_root, *Path(uri[1:]).parts[:-1], current_dir.label]
    #         #print(str(Path(*Path(uri).parts[:10])))
    #         #temp_scan = session.get_object(str(Path(*Path(uri).parts[:10])))
    #         temp_scan = session.experiments[Path(uri[1:]).parts[8]]  # FIXME
    #         temp_path[5] = subject_label
    #         temp_path[7] = session.experiments[temp_scan.image_session_id].label
    #         temp_path[9] = '-'.join((temp_scan.id,temp_scan._overwrites['type']))
    #         path = Path(*temp_path)
    #         logging.warning(f'RESOURCE PATH: {path}')
    #         #temp_path[7] = session.create_object(Path(uri).parts[:7]).label
    #     else:
    #         path = Path(local_root, current_dir.uri[1:])
    #         logging.warning(f'OTHER PATH: {path}')

    #     if not path.is_dir():
    #         try:
    #             session.create_object(uri).download_dir(Path(*path.parts[:-3]))
    #         except Exception as e:
    #             logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
    #             logging.debug(f'Original exception: {e}')
        
    #     match current_dir:
    #         case session.classes.SubjectData():
    #             child_attr = 'experiments'
    #         case session.classes.MrSessionData():
    #             child_attr = 'scans'
    #         case session.classes.MrScanData():
    #             child_attr = 'resources'
    #         case session.classes.ResourceCatalog():
    #             child_attr = 'files'
    #         case session.classes.FileData():
    #             child_attr = None
    #         case _:
    #             child_attr = None

    #     if child_attr and hasattr(current_dir, child_attr):
    #         for child_dir in getattr(current_dir,child_attr).values():
    #             self._download_recur(session=session, current_dir=child_dir, local_root=local_root, subject_label=subject_label)
    #     else:
    #         logging.debug(f'Reached end of subject directory structure: {current_dir}')
    #         return



    # recursively check if directory/file exists
    # if no, try to download
    # if yes, check if child directories/files exists
    # etc.
    # def _download_recur1(self, session, parent_dir, local_root: str, **kwargs) -> None:
        
    #     match parent_dir:
    #         # case session.classes.ProjectData():
    #         #     child_attr = 'subjects'
    #         case session.classes.SubjectData():
    #             child_attr = 'experiments'
    #         case session.classes.MrSessionData():
    #             child_attr = 'scans'
    #         case session.classes.MrScanData():
    #             child_attr = 'resources'
    #         case session.classes.ResourceCatalog():
    #             child_attr = 'files'
    #         case session.classes.FileData():
    #             child_attr = None
    #         case _:
    #             child_attr = None

    #     #for attr in ('subjects','experiments','scans','resources'):
    #     if child_attr and hasattr(parent_dir, child_attr):
    #         for child_dir in getattr(parent_dir,child_attr).values():
    #             uri = child_dir.uri

    #             if isinstance(child_dir, session.classes.SubjectData):
    #                 subject_label = child_dir.label
    #                 path = Path(local_root, *Path(uri[1:]).parts[:-1], subject_label)
    #                 logging.warning(f'PROJECT PATH: {path}')
    #             elif isinstance(child_dir, session.classes.MrSessionData):
    #                 temp_path = [local_root, *Path(uri[1:]).parts[:-1], child_dir.label]
    #                 temp_path[5] = subject_label
    #                 path = Path(*temp_path)
    #                 logging.warning(f'EXP PATH: {path}')
    #             elif isinstance(child_dir, session.classes.MrScanData):
    #                 temp_path = [local_root, *Path(uri[1:]).parts[:-1], '-'.join((child_dir.id,child_dir._overwrites['type']))]
    #                 temp_path[5] = subject_label
    #                 temp_path[7] = session.experiments[child_dir.image_session_id].label
    #                 path = Path(*temp_path)
    #                 logging.warning(f'SCAN PATH: {path}')
    #             elif isinstance(child_dir, session.classes.ResourceCatalog):
    #                 temp_path = [local_root, *Path(uri[1:]).parts[:-1], child_dir.label]
    #                 #print(str(Path(*Path(uri).parts[:10])))
    #                 #temp_scan = session.get_object(str(Path(*Path(uri).parts[:10])))
    #                 temp_scan = session.experiments[Path(uri[1:]).parts[8]]
    #                 temp_path[5] = subject_label
    #                 temp_path[7] = session.experiments[temp_scan.image_session_id].label
    #                 temp_path[9] = '-'.join((temp_scan.id,temp_scan._overwrites['type']))
    #                 path = Path(*temp_path)
    #                 logging.warning(f'RESOURCE PATH: {path}')
    #                 #temp_path[7] = session.create_object(Path(uri).parts[:7]).label
    #             else:
    #                 path = Path(local_root, child_dir.uri[1:])
    #                 logging.warning(f'OTHER PATH: {path}')

    #             if not path.is_dir():
    #                 try:
    #                     session.create_object(uri).download_dir(Path(*path.parts[:-3]))
    #                 except Exception as e:
    #                     logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
    #                     logging.debug(f'Original exception: {e}')
                        
    #             self._download_recur(session=session, parent_dir=child_dir, local_root=local_root, subject_label=subject_label)

    #     else:
    #         logging.debug(f'Reached end of subject directory structure: {parent_dir}')
    #         return


    # def _download(self, session, parent_dir) -> None:

    #     try:
    #         session.create_object(parent_dir.custom_variables['uri']).download_dir(parent_dir.custom_variables['path'].parts[:-3])
    #     except:
    #         print('problemo')
    #         for child_dir in parent_dir.custom_variables['children']:
    #             self._download(session=session,parent_dir=child_dir)


        # try:
        #     session.create_object('/'+object_path).download_dir(local_path)
        # except zipfile.BadZipFile as e:
        #     logging.warning(f'Problem zipping/unzipping: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
        #     continue
        # except Exception as e:
        #     logging.warning(f'Problem downloading: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
        #     continue
