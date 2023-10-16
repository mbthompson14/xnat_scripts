

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
import zipfile
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

                self._test_download(session=session, parent_dir=project, local_root=local_root)

                # self._upload

            else:
                raise Exception("Project ID not found in XNAT")
            



    def _test_download(self, session, parent_dir, local_root: str) -> None:

        for attr in ('subjects','experiments','scans','resources'):
            if hasattr(parent_dir, attr):
                for child_dir in getattr(parent_dir,attr).values():
                    uri = Path(child_dir.uri)
                    path = Path(local_root)






    def _check_download(self, session, project, local_root: str) -> None:

        for subject in project.subjects.values():

            subject.custom_variables['children'] = subject.experiments.values()

            subject.custom_variables['uri'] = Path('/data','projects',project.id,'subjects',subject.label)
            #print(f'uri: {str(subject_uri)}')
            subject.custom_variables['path'] = Path(local_root,'data','projects',project.id,'subjects',subject.label)
            #print(f'path: {str(subject_path)}')

            if subject.custom_variables['path'].is_dir():
                #print('subject path exists')
                for exp in subject.custom_variables['children']:
                    exp_uri = Path(subject.custom_variables['uri'],'experiments',exp.id)
                    #print(f'uri: {str(uri)}')
                    exp_path = Path(subject.custom_variables['path'],'experiments',exp.label)
                    #print(f'path: {str(path)}')

                    if exp_path.is_dir():
                        #print('exp path exists')
                        for scan in exp.scans.values():
                            scan_uri = Path(exp_uri,'scans',scan.id)
                            scan_path = Path(exp_path,'scans','-'.join((scan.id,scan._overwrites['type'])))
                            #print(f'scan path: {path}')

                            if scan_path.is_dir():
                                for resource in scan.resources.values():
                                    resource_uri = Path(scan_uri,'resources',resource.id)
                                    resource_path = Path(scan_path,'resources',resource.label)

                                    if resource_path.is_dir():
                                        for file in resource.files:
                                            file_uri = Path(resource_uri,'files',file)
                                            #print(f'##### URI: {file_uri}')
                                            file_path = Path(resource_path,'files',file)
                                            #print(f'##### PATH: {file_path}')

                                            if file_path.is_file():
                                                continue
                                            else:
                                                pass
                                                # download file

                                    else:
                                        pass
                            else:
                                pass
                    else:
                        pass
            else:
                self._download(session=session,parent_dir=subject)

    def _download(self, session, parent_dir) -> None:

        try:
            session.create_object(parent_dir.custom_variables['uri']).download_dir(parent_dir.custom_variables['path'].parts[:-3])
        except:
            print('problemo')
            for child_dir in parent_dir.custom_variables['children']:
                self._download(session=session,parent_dir=child_dir)


        # try:
        #     session.create_object('/'+object_path).download_dir(local_path)
        # except zipfile.BadZipFile as e:
        #     logging.warning(f'Problem zipping/unzipping: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
        #     continue
        # except Exception as e:
        #     logging.warning(f'Problem downloading: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
        #     continue
