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
        
        with self._xnat.connect(host, loglevel='INFO') as session:
            if project_id in session.projects:
                project = session.projects[project_id]
            else:
                raise Exception("Project ID not found in XNAT")

            xnat_paths = self.list_xnat_objects(project)

            to_download = xnat_paths - local_paths

            for download_path in to_download:
                #download_path += '.zip'
                try:
                    #TODO: NEED TO CREATE DIRECTORY FIRST?
                    # OR MAYBE IT CANT FIND THE PATH ON XNAT
                    session.download(uri=download_path,target=Path(local,download_path))
                except Exception as e:
                    raise Exception(f'Error downloading file: {download_path}\nOriginal exception: {e}')
            
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

    
    def list_xnat_objects(self, project: str) -> set():

        xnat_paths = set()

        # might be a more efficient way to do this
        for subject in project.subjects.values():
            for exp in subject.experiments.values():
                for scan in exp.scans.values():
                    for resource in scan.resources.values():
                        for file in resource.files.values():
                            xnat_paths.add(file.uri)
        
        return xnat_paths