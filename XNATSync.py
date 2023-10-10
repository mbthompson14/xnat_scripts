import os
import xnat
import tempfile
from pathlib import Path
from zipfile import ZipFile

# https://stackoverflow.com/questions/56892490/sync-local-folder-to-s3-bucket-using-boto3

class XNATSync:
    def __init__(self) -> None:
        self._xnat = xnat

    def sync(self, host: str, project_id: str, local: str) -> None:
        """
        Sync local & xnat
        """

        local_paths = self.list_local_objects(local_folder=local)
        #print(local_paths)

        with self._xnat.connect(host, loglevel='INFO') as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                xnat_paths = self.list_xnat_objects(session, project)

                to_download = xnat_paths - local_paths
                to_upload = local_paths - xnat_paths

                #self.download(session, local, to_download)
                self.upload(session, local, to_upload)

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
        
        print(f'local paths: {paths}')
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
        print(f'xnat paths: {xnat_paths}')
        return xnat_paths

    def download(self, session, local: str, to_download: set()) -> None:
        for download_path in to_download:
                    try:
                        local_path = Path(local,download_path)

                        try:
                            local_path.parent.mkdir(parents=True,exist_ok=True)
                        except Exception as e:
                             raise Exception(f'Problem creating local directory: {local_path.parent}\nOriginal exception: {e}')
                        
                        session.download(uri='/'+download_path,target=local_path)

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

                        
                        # with tempfile.TemporaryFile() as temp_path:
                        #     session.download_stream('/' + download_path, temp_path, format='zip', verbose=True)

                        #     with ZipFile(temp_path) as zip_file:
                        #         zip_file.extractall(local_path)
                        
                    except Exception as e:
                        raise Exception(f'Problem downloading file: {download_path}\nOriginal exception: {e}')

    def upload(self, session, local: str, to_upload: set()) -> None:

        unique_scans = set([str(Path(*Path(path).parts[:9])) for path in to_upload])

        # TODO zip each scan folder

        # TODO how to upload individual files in the correct format

        for upload_path in unique_scans:  # for individual upload, upload_path in to_upload
            try:
                local_path = Path(local,upload_path)
                parts = list(Path(upload_path).parts)
                parts[0] = 'archive'
                parts[2] = 'sync_test'
                session.services.import_(local_path, 
                                        #project='sync_test', 
                                        destination='/'+str(Path(*parts)),
                                        overwrite = 'none',
                                        import_handler = 'SI')
            except Exception as e:
                 raise Exception(f'Problem with file: {upload_path}\nOriginal exception: {e}')

