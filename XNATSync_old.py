import sys
import xnat
import zipfile
import logging
from pathlib import Path
from datetime import datetime


# Will likely need to reference xnat objects rather than just uri's
# e.g. to access experiment/scan/resource labels rather than ID
# object properties: uri, id, label
# uri contains id, not label

# download order:
# project?
# subject
# experiment?
# scan
# resource
# file


# NEW PLAN:
# Deal with one subject at a time
# save both ID and label for each level
# maintain both the URI and path for each directory/file

# for participant in project
# get participant label, id
# build participant URI & path
# check if particpant exists in local directory
# if exists -> check experiments......
# if not, try to download_dir participant

# if particpant download fails, download exp......




# https://stackoverflow.com/questions/56892490/sync-local-folder-to-s3-bucket-using-boto3

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
        """
        Sync local & xnat
        """

        logging.info('Starting sync...')

        local_paths = self.list_local_objects(local_folder=local_root)
        #print(local_paths)

        with self._xnat.connect(host, loglevel=logging.root.level) as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                xnat_paths = self.list_xnat_objects(session, project)

                logging.info(f'local_paths: {len(local_paths)}, e.g. {sorted(local_paths)[0] if len(local_paths) else None}')
                logging.info(f'xnat_paths: {len(xnat_paths)}, e.g. {sorted(xnat_paths)[0] if len(xnat_paths) else None}')

                to_download = list(xnat_paths - local_paths)
                logging.info(f'to_download: {len(to_download)}, e.g. {sorted(to_download)[0] if len(to_download) else None}')
                # to_upload = local_paths - xnat_paths
                # print(f'to_upload: {to_upload}')

                self.download(session=session, local_root=local_root, to_download=to_download)
                #self.upload(session, local, to_upload)

            else:
                raise Exception("Project ID not found in XNAT")

    @staticmethod
    def list_local_objects(local_folder: str) -> set():
        
        path = Path(local_folder)

        paths = set()

        for file_path in path.rglob("*"):
            if file_path.is_dir() or file_path.name == '.DS_Store':
                continue
            str_file_path = str(file_path)
            str_file_path = str_file_path.replace(f'{str(path)}/','')
            paths.add(str_file_path)
        return paths

    
    def list_xnat_objects(self, session, project: str) -> set():

        xnat_paths = set()

        #TODO might be a more efficient way to do this
        # for subject in project.subjects.values():
        #     for exp in subject.experiments.values():
        #         for scan in exp.scans.values():
        #             for resource in scan.resources.values():
        #                 for file in resource.files.values():
        #                     xnat_paths.add(file.uri[1:])
        
        # maybe use cURL for this?

        # ~2x faster
        for subject in project.subjects.values():
                for exp in subject.experiments.values():
                    uri = f'/data/projects/{project.id}/subjects/{subject.label}/experiments/{exp.id}/scans/ALL/files'
                    for file in session.get_json(uri)['ResultSet']['Result']:
                        adj_uri = list(Path(file['URI'][1:]).parts)
                        adj_uri.insert(1,f'projects/{project.id}/subjects/{subject.label}')
                        xnat_paths.add(str(Path(*adj_uri)))
        return xnat_paths

    def download(self, session, local_root: str, to_download: list()) -> None:

        #to_download = self._download_folder(session=session, to_download=to_download, dir_level='subject', local_root=local_root)

        # zip, download, unzip new scan folders
        to_download = self._download_folder(session=session, to_download=to_download, dir_level='scan', local_root=local_root)

        logging.info(f'to_download after downloading scans: {len(to_download)}, e.g. {sorted(to_download)[0] if len(to_download) else None}')

        # dowload remaining individual files
        # for download_path in to_download:
        #             try:
        #                 local_path = Path(local_root,download_path)
        #                 local_path.parent.mkdir(parents=True,exist_ok=True)
        #                 session.download(uri='/'+download_path,target=local_path)

                        #TODO need to find a faster way to download
                        # zip individual files then unzip?
                        # zip resource folders and unzip? could use download_dir for this

                        # faster method: (will this work for upload as well?)
                        # first, download all new subjects as zip files
                            # get unique paths by subject
                            # check if path exists in local directory
                            # if not, download_dir that subject
                            # ignore paths with that subject going forward
                        # second, download all new scans that were not part of the new subjects
                            # of remaining paths, get unique paths by scan (i.e. truncate after scan id)
                            # check if path exists in local directory
                            # if not, download_dir that scan
                            # ignore paths of those scans going forward
                        # third, download all new files that were not part of the new scans
                            # download remaining paths individually

                    # add the directory does not exist exception here
                    # except Exception as e:
                    #     logging.warning(f'Problem downloading file/directory: {download_path}\nOriginal exception: {e}')
                    #     continue

    def _download_folder(self, session, to_download: list(), dir_level: str, local_root: str) -> set():

        if dir_level == 'subject':
            dir_level = 4
        elif dir_level == 'scan':
            dir_level = 8
        else:
            raise Exception('Download directory level invalid value. Allowed values: ["subject","scan"]')

        unique_objects = set([str(Path(*Path(path).parts[:dir_level+1])) for path in to_download])
        logging.info(f'unique_{dir_level}: {len(unique_objects)}, e.g. {sorted(unique_objects)[0] if len(unique_objects) else None}')

        for object_path in unique_objects:
            local_path = Path(local_root,str(Path(*Path(object_path).parts[:6])))
            logging.info(f'obect_path: {object_path}, local_path: {local_path}')
            # if Path(local_path).exists():
            #     continue
            # else:
            object_id = Path(object_path).parts[dir_level]
            #local_path.mkdir(parents=True)
            try:
                session.create_object('/'+object_path).download_dir(local_path)
            except zipfile.BadZipFile as e:
                logging.warning(f'Problem zipping/unzipping: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
                continue
            except Exception as e:
                logging.warning(f'Problem downloading: {object_path}. Will try downloading individual files.\nOriginal exception: {e}')
                continue
            else:
                to_download = [path for path in to_download if Path(path).parts[dir_level] != object_id]

        return to_download

    def upload(self, session, local: str, to_upload: set()) -> None:

        unique_exp = set([str(Path(*Path(path).parts[:7])) for path in to_upload])
        logging.info(f'unique_exp: {unique_exp}')

        # /archive/projects/PROJECT/subjects/SUBJECT/experiments/LABEL
        # Importing to this url would allow you to merge content with previously archived data.

        # import_dir by experiment may handle conflicting files by itself

        for upload_path in unique_exp:  # for individual upload, upload_path in to_upload
            try:
                local_path = Path(local,upload_path)
                parts = list(Path(upload_path).parts)
                parts[0] = 'archive'
                parts[2] = 'sync_test'
                session.services.import_dir(local_path, 
                                        destination='/'+str(Path(*parts)),
                                        overwrite = 'none')
                
            except xnat.exceptions.XNATUploadError as e:
                logging.exception(f'Problem uploading file/directory: {upload_path}\nOriginal exception: {e}')
                continue
            except Exception as e:
                logging.exception(f'Unknown problem uploading file/directory: {upload_path}\nOriginal exception: {e}')
                continue

