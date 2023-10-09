import os
import xnat
from pathlib import Path

# https://stackoverflow.com/questions/56892490/sync-local-folder-to-s3-bucket-using-boto3

class XNATSync:
    def __init__(self) -> None:
        self._xnat = xnat

    def sync(self, host: str, project_id: str, local: str) -> None:
        """
        Sync source to dest
        """

        local_paths = self.list_local_objects(local_folder=local)
        #print(local_paths)

        with self._xnat.connect(host, loglevel='INFO') as session:
            if project_id in session.projects:
                project = session.projects[project_id]
                xnat_paths = self.list_xnat_objects(session, project)

                to_download = xnat_paths - local_paths

                for download_path in to_download:
                    #download_path += '.zip'
                    try:
                        local_path = Path(local,download_path)
                        local_path.parent.mkdir(parents=True,exist_ok=True)
                        uri = '/' + download_path
                        #session.download(uri=uri,target=local_path)
                        session.download(uri=uri,target=local_path)

                        #TODO need to find a faster way to download
                        # zip individual files then unzip?
                        # zip resource folders and unzip? could use download_dir for this

                        """
                        with tempfile.TemporaryFile() as temp_path:
                            self.xnat_session.download_stream(self.uri + '/files', temp_path, format='zip', verbose=verbose)

                        with ZipFile(temp_path) as zip_file:
                            zip_file.extractall(target_dir)
                        """

                    except Exception as e:
                        raise Exception(f'Error downloading file: {download_path}\nOriginal exception: {e}')
            else:
                raise Exception("Project ID not found in XNAT")
            
    @staticmethod
    def list_local_objects(local_folder: str) -> set():
        
        path = Path(local_folder)

        paths = set()

        for file_path in path.rglob("*"):
            if file_path.is_dir():
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
        
        # slightly faster
        for subject in project.subjects.values():
                for exp in subject.experiments.values():
                    for scan in exp.scans.values():
                         uri = f'/data/projects/{project}/subjects/{subject.label}/experiments/{exp.id}/scans/{scan.id}/files'
                         for file in session.get_json(uri)['ResultSet']['Result']:
                              xnat_paths.add(file['URI'])

        return xnat_paths