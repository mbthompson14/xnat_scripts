#!/Users/matthew/Repos/xnat_scripts/venv/bin/python3

# /nox2/data/RAD22/Scripts/DataOrg/xnat_scripts/venv/bin/python3

"""
grab a specific dataset (single session) from XNAT

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
    grab_data(server, args.database, args.session, args.binary)

def grab_data(server: str, database: str, session: str, binary: bool) -> None:
    
    logging.info(f'Running {Path(__file__).name}')
    logging.info(f'Database: {database}')
    logging.info(f'Session: {session}')

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
                    if binary:  # if binary flag, download binary data only (from subject level)
                        for file in project.experiments[session].subject.files.values():    
                            try:
                                print(f'Attempting to download {file.name} to current directory')
                                logging.info(f'Attempting to download {file.name} to current directory')
                                file.download(file.name)  # download file
                            except:
                                print(f'Error downloading file {file.name}')
                                logging.error(f'Error downloading file {file.name}')
                            else:
                                print(f'Successfully downloaded: {file.name}')
                                logging.info(f'Successfully downloaded: {file.name}')
                    else:
                        # attempt to download the session
                        try:
                            print(f'Attempting to download session {session} to current directory')
                            logging.info(f'Attempting to download session {session} to current directory')
                            project.experiments[session].download(f'./{session}.zip')
                        except:
                            print(f'Error downloading session {session} data')
                            logging.error(f'Error downloading session {session} data')
                            sys.exit(3)
                        else:
                            # successfully downloaded
                            print(f'Successfully downloaded: {session}')
                            logging.info(f'Successfully downloaded: {session}')
                else:
                    # could not find session in this project
                    print('XNAT session does not exist')
                    logging.error('XNAT session does not exist')
                    sys.exit(2)
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
    returns (downloads) all data for a given session into the present working directory

example:
    grabXNATdata -x RADCO -s RAD22_GSU_BU101_140AKd_01

error exit codes:
    0 - no error
    1 - XNAT database does not exist
    2 - XNAT session does not exist
    3 - Session download error
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
        help='session to grab'
    )

    parser.add_argument(
        '-b','--binary',
        help='grab binary data only',
        action='store_true'
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