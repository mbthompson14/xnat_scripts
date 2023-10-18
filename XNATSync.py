

"""

connect to xnat
create project object from project id

for subject in project:
    URI = 
    path = 
    does this subject exist in the local directory (check path)?
        if yes:
            check if exp exists
                check if scans exists
                    check if resources exist
                        check if files exist
        if no:
            try:
                download_dir this subject (URI) to local directory (path)
            except:
                log warning
                for exp in subject:
                    try to download exp
                    except:
                        log warning
                        for scan in exp:
                            try to download scan
                            except:
                                log warning
                                for resource in scan:
                                    try to download resource
                                    except:
                                        log warning
                                        for file in resource:
                                            try to download file
                                            except:
                                                log error

"""


import xnat
import logging
#import zipfile
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
    
    def sync(self, host: str, project_id: str, local_root: str) -> None:

        logging.info('Starting sync...')

        with self._xnat.connect(host, loglevel=logging.root.level) as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                
                for subject in project.subjects.values():
                    self._download_recur(session=session, current_dir=subject, local_root=local_root)

                # self._upload

            else:
                raise Exception("Project ID not found in XNAT")
            

    def _download_recur(self, session, current_dir, local_root: str, subject_label: str = None) -> None:

        # get URI
        uri = current_dir.uri

        # get local path
        if isinstance(current_dir, session.classes.SubjectData):
            subject_label = current_dir.label
            path = Path(local_root, *Path(uri[1:]).parts[:-1], subject_label)
            logging.warning(f'SUBJECT PATH: {path}')
        elif isinstance(current_dir, session.classes.MrSessionData):
            temp_path = [local_root, *Path(uri[1:]).parts[:-1], current_dir.label]
            temp_path[5] = subject_label
            path = Path(*temp_path)
            logging.warning(f'EXP PATH: {path}')
        elif isinstance(current_dir, session.classes.MrScanData):
            temp_path = [local_root, *Path(uri[1:]).parts[:-1], '-'.join((current_dir.id,current_dir._overwrites['type']))]
            temp_path[5] = subject_label
            temp_path[7] = session.experiments[current_dir.image_session_id].label
            path = Path(*temp_path)
            logging.warning(f'SCAN PATH: {path}')
        elif isinstance(current_dir, session.classes.ResourceCatalog):
            temp_path = [local_root, *Path(uri[1:]).parts[:-1], current_dir.label]
            #print(str(Path(*Path(uri).parts[:10])))
            #temp_scan = session.get_object(str(Path(*Path(uri).parts[:10])))
            temp_scan = session.experiments[Path(uri[1:]).parts[8]]  # FIXME
            temp_path[5] = subject_label
            temp_path[7] = session.experiments[temp_scan.image_session_id].label
            temp_path[9] = '-'.join((temp_scan.id,temp_scan._overwrites['type']))
            path = Path(*temp_path)
            logging.warning(f'RESOURCE PATH: {path}')
            #temp_path[7] = session.create_object(Path(uri).parts[:7]).label
        else:
            path = Path(local_root, current_dir.uri[1:])
            logging.warning(f'OTHER PATH: {path}')

        if not path.is_dir():
            try:
                session.create_object(uri).download_dir(Path(*path.parts[:-3]))
            except Exception as e:
                logging.warning(f'Problem downloading: {path}. Will try downloading as sub-directories/files.')
                logging.debug(f'Original exception: {e}')
        
        match current_dir:
            case session.classes.SubjectData():
                child_attr = 'experiments'
            case session.classes.MrSessionData():
                child_attr = 'scans'
            case session.classes.MrScanData():
                child_attr = 'resources'
            case session.classes.ResourceCatalog():
                child_attr = 'files'
            case session.classes.FileData():
                child_attr = None
            case _:
                child_attr = None

        if child_attr and hasattr(current_dir, child_attr):
            for child_dir in getattr(current_dir,child_attr).values():
                self._download_recur(session=session, current_dir=child_dir, local_root=local_root, subject_label=subject_label)
        else:
            logging.debug(f'Reached end of subject directory structure: {current_dir}')
            return



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
            






    # def _check_download_old(self, session, project, local_root: str) -> None:

    #     for subject in project.subjects.values():

    #         subject.custom_variables['children'] = subject.experiments.values()

    #         subject.custom_variables['uri'] = Path('/data','projects',project.id,'subjects',subject.label)
    #         #print(f'uri: {str(subject_uri)}')
    #         subject.custom_variables['path'] = Path(local_root,'data','projects',project.id,'subjects',subject.label)
    #         #print(f'path: {str(subject_path)}')

    #         if subject.custom_variables['path'].is_dir():
    #             #print('subject path exists')
    #             for exp in subject.custom_variables['children']:
    #                 exp_uri = Path(subject.custom_variables['uri'],'experiments',exp.id)
    #                 #print(f'uri: {str(uri)}')
    #                 exp_path = Path(subject.custom_variables['path'],'experiments',exp.label)
    #                 #print(f'path: {str(path)}')

    #                 if exp_path.is_dir():
    #                     #print('exp path exists')
    #                     for scan in exp.scans.values():
    #                         scan_uri = Path(exp_uri,'scans',scan.id)
    #                         scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'])))
    #                         #print(f'scan path: {path}')

    #                         if scan_path.is_dir():
    #                             for resource in scan.resources.values():
    #                                 resource_uri = Path(scan_uri,'resources',resource.id)
    #                                 resource_path = Path(scan_path,'resources',resource.label)

    #                                 if resource_path.is_dir():
    #                                     for file in resource.files:
    #                                         file_uri = Path(resource_uri,'files',file)
    #                                         #print(f'##### URI: {file_uri}')
    #                                         file_path = Path(resource_path,'files',file)
    #                                         #print(f'##### PATH: {file_path}')

    #                                         if file_path.is_file():
    #                                             continue
    #                                         else:
    #                                             pass
    #                                             # download file

    #                                 else:
    #                                     pass
    #                         else:
    #                             pass
    #                 else:
    #                     pass
    #         else:
    #             self._download(session=session,parent_dir=subject)

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
