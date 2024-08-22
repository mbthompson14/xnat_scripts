#!/Users/matthew/Repos/xnat_scripts/venv/bin/python3

# /nox2/data/RAD22/Scripts/DataOrg/xnat_scripts/venv/bin/python3

"""
push a specific dataset (single session) to XNAT

matthew thompson
2024.08

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

    # call grab function
    push_data(server, args.database, args.session)

def push_data(server: str, database: str, session: str) -> None:
    
    logging.info(f'Running {Path(__file__).name}')
    logging.info(f'Database: {database}')
    logging.info(f'Session: {session}')

    # set path to session, expect to find it in the present dir
    # abort if it doesn't exist
    local_path = Path(f'./{session}')
    if not local_path.exists():
        print(f'Local session {local_path} does not exist')
        logging.error(f'Local session {local_path} does not exist')
        sys.exit(2)

    try:
        # attempt to connect to the server
        connection = xnat.connect(
            server=server, 
            default_timeout = 600, 
            loglevel=logging.root.level,
            logger=logging.getLogger())
    except:
        # problem connecting to server, abort
        print('XNAT connection error')
        logging.error('XNAT connection error')
        sys.exit(9)
    else:
        # successfully connected to server
        with connection:
            # look for project on the server
            if database in connection.projects:
                # project found
                project = connection.projects[database]

                # look for session in the project
                if session in project.experiments:
                    # session already exists in this project, abort
                    print('XNAT session already exists')
                    logging.error('XNAT session already exists')
                    sys.exit(3)
                else:
                    # attempt to upload session
                    try:
                        connection.services.import_dir(local_path)
                    except:
                        # problem uploading
                        print('Error uploading session data')
                        logging.error('Error uploading session data')
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
        description="grab a specific dataset (single session) from XNAT",
        epilog='''
output:
    push a session from the present working directory into XNAT

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