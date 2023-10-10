from XNATSync import XNATSync

def main():
    HOST = 'https://xnatccn.semel.ucla.edu/'
    PROJECT_ID = 'sync_test'
    LOCAL = 'RawDICOM'
    LOGS = 'LOGS'

    sync = XNATSync()
    sync.sync(host=HOST,project_id=PROJECT_ID,local=LOCAL)

if __name__ == '__main__':
    main()



"""
Traceback (most recent call last):
  File "/Users/matthew/Repos/radco/XNATSync.py", line 30, in sync
    session.download(uri=download_path,target=Path(local,download_path))
  File "/Users/matthew/anaconda3/lib/python3.10/site-packages/xnat/session.py", line 722, in download
    with open(target, 'wb') as out_fh:
FileNotFoundError: [Errno 2] No such file or directory: '/data/projects/RADCO/subjects/XNAT_S04887/experiments/XNAT_E04884/scans/47/resources/5234/files/MR.1.3.12.2.1107.5.2.43.166010.2023032016050412509012675.dcm'

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/Users/matthew/Repos/radco/test.py", line 12, in <module>
    main()
  File "/Users/matthew/Repos/radco/test.py", line 9, in main
    sync.sync(host=HOST,project_id=PROJECT_ID,local=LOCAL)
  File "/Users/matthew/Repos/radco/XNATSync.py", line 32, in sync
    raise Exception(f'Error downloading file: {download_path}\nOriginal exception: {e}')
Exception: Error downloading file: /data/projects/RADCO/subjects/XNAT_S04887/experiments/XNAT_E04884/scans/47/resources/5234/files/MR.1.3.12.2.1107.5.2.43.166010.2023032016050412509012675.dcm
Original exception: [Errno 2] No such file or directory: '/data/projects/RADCO/subjects/XNAT_S04887/experiments/XNAT_E04884/scans/47/resources/5234/files/MR.1.3.12.2.1107.5.2.43.166010.2023032016050412509012675.dcm'
(base) matthew@PSYCH-GEN-186 radco % 

 #TODO: NEED TO CREATE DIRECTORY FIRST?
 # OR MAYBE IT CANT FIND THE PATH ON XNAT
"""