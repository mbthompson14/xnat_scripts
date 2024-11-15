#!/Users/matthew/Repos/xnat_scripts/venv/bin/python3

# /nox2/data/RAD22/Scripts/DataOrg/xnat_scripts/venv/bin/python3

"""
push a specific dataset (single session) to XNAT

matthew thompson
2024.08

TODO how to name binary name folder 'nonDICOM'

"""

import sys
import xnat
import argparse
import logging
from pathlib import Path
from util import logging_setup, config

CONFIG = 'test_config.json'

def main():
    # get args
    args = parse_args()

    # get server address, exp dir
    server, exp_dir = config(CONFIG)

    # setup logging
    logging_setup(
        log_level=args.debug, 
        logs_dir=f"{exp_dir}/LOGS/XNAT", 
        name=Path(__file__).stem)

    # call push function
    push_data(server, exp_dir, args.database, args.session, args.binary)

def push_data(server: str, exp_dir: str, database: str, session: str, file: str) -> None:
    
    logging.info(f'Running {Path(__file__).name}')
    logging.info(f'Database: {database}')
    logging.info(f'Session: {session}')

    # before we connect to the server, make sure we can find the local dir/file
    # set path to session or binary file, expect to find it in the experiment dir
    if file:
        local_path = Path(f'{exp_dir}/{file}')
    else:
        local_path = Path(f'{exp_dir}/{session}')  # must be a directory (not zipped)
    # abort if it doesn't exist
    if not local_path.exists():
        print(f'Local session {local_path} does not exist')
        logging.error(f'Local session {local_path} does not exist')
        sys.exit(2)

    # attempt to connect to the server
    try:
        connection = xnat.connect(
            server=server, 
            default_timeout = 600, 
            loglevel=logging.root.level,
            logger=logging.getLogger())
    except Exception as e:
        # problem connecting to server, abort
        print('XNAT connection error, see log file for details')
        logging.error(f'XNAT connection error: {e}')
        sys.exit(9)
    else:
        # successfully connected to server
        with connection:
            # look for project on the server
            if database in connection.projects:
                # project found
                project = connection.projects[database]
                # if we're uploading a file
                if file:
                    if file in project.experiments[session].subject.files:
                        # file already exists in this session, abort
                        print('file already exists in this session')
                        logging.error('file already exists in this session')
                        sys.exit(3)
                    else:
                        upload_uri = f'/data/projects/{database}/subjects/{project.experiments[session].subject.label}/files/{file}'
                        try:
                            print(f'Attempting to upload: {local_path}')
                            logging.info(f'Attempting to upload: {local_path}')
                            connection.upload_file(uri=upload_uri, path=local_path, overwrite=False)
                        except Exception as e:
                            # problem uploading
                            print('Error uploading file, see log file for details')
                            logging.error(f'Error uploading file: {e}')
                            sys.exit(4)
                        else:
                            # succeful upload
                            print(f'Successfully uploaded: {upload_uri}')
                            logging.info(f'Successfully uploaded: {upload_uri}')
                # else we're uploading a session
                else:
                    # look for session in the project
                    if session in project.experiments:
                        # session already exists in this project, abort
                        print('XNAT session already exists')
                        logging.error('XNAT session already exists')
                        sys.exit(3)
                    else:
                    # attempt to upload session
                        try:
                            print(f'Attempting to upload: {local_path}')
                            logging.info(f'Attempting to upload: {local_path}')
                            connection.services.import_dir(directory=local_path, project=project)
                            #connection.services.import_(path=local_path, project=project)
                        except Exception as e:
                            # problem uploading
                            print('Error uploading session data, see log file for details')
                            logging.error(f'Error uploading session data: {e}')
                            sys.exit(4)
                        else:
                            # succeful upload
                            print(f'Successfully uploaded: {session}')
                            logging.info(f'Successfully uploaded: {session}')
            else:
                # could not find project on the server
                print('XNAT database does not exist')
                logging.error('XNAT database does not exist')
                sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(
        prog=Path(__file__).name,
        description="push a specific dataset (single session) to XNAT",
        epilog='''
output:
    push a session from the experiment directory to XNAT

example:
    pushXNATdata -x RADCO -s RAD22_UCLA_BU101_180AKd_01

error exit codes:
    0 - no error
    1 - XNAT database does not exist
    2 - local session does not exist
    3 - XNAT session already exists
    4 - error uploading data
    9 - XNAT connection error

        ''',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-x','--database',
        help='name of XNAT database to query'
    )

    parser.add_argument(
        '-s','--session',
        help='session to push'
    )

    parser.add_argument(
        '-b','--binary',
        help='push the named binary file (TWIX or PDF)'
    )

    parser.add_argument(
        '-d','--debug',
        help='debug',
        action='store_true'
    )

    # parse args
    return parser.parse_args()


if __name__ == "__main__":
    main()