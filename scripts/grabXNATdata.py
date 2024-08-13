#!/Users/matthew/Repos/xnat_scripts/env/bin/python3

# /nyx3/data/RAD22/Scripts/DataOrg/xnat_scripts/env/bin/python3

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
    # (1) get args
    args = parse_args()

    # (2) get server, exp dir
    server, exp_dir = config(CONFIG)

    # (3) setup logging
    logging_setup(
        log_level=args.debug, 
        logs_dir=f"{exp_dir}/logs", 
        name=Path(__file__).stem)

    # (4) call grab function
    grab_data(server, args.database, args.session)

def grab_data(server: str, database: str, session: str) -> None:
    
    logging.info(f'Running {Path(__file__).name}')
    logging.info(f'Database: {database}')
    logging.info(f'Session: {session}')

    try:
        connection = xnat.connect(
            server=server, 
            default_timeout = 600, 
            loglevel=logging.root.level,
            logger=logging.getLogger())
    except:
        logging.error('XNAT connection error')
        sys.exit(9)
    else:
        with connection:
            if database in connection.projects:
                project = connection.projects[database]

                if session in project.experiments:
                    try:
                        project.experiments[session].download(f'./{session}.zip')
                    except:
                        print('Error downloading session data')
                        logging.error('Error downloading session data')
                        sys.exit(3)
                    else:
                        print(f'Successfully downloaded: {session}')
                        logging.info(f'Successfully downloaded: {session}')
                else:
                    print('XNAT session does not exist')
                    logging.error('XNAT session does not exist')
                    sys.exit(2)
            else:
                print('XNAT database does not exist')
                logging.error('XNAT database does not exist')
                sys.exit(1)

# def list_data(server, database):

#     logging.info(f'Running {Path(__file__).name}')

#     try:
#         connection = xnat.connect(
#             server=server, 
#             default_timeout = 600, 
#             loglevel=logging.root.level,
#             logger=logging.getLogger())
#     except:
#         logging.error('XNAT connection error')
#         sys.exit(9)
#     else:
#         with connection:
#             if database in connection.projects:
#                 project = connection.projects[database]

#                 for session in project.experiments.values():
#                     print(session.label)
#                     logging.info(f'[session]: {session.label}')

#             else:
#                 logging.error('XNAT database does not exist')
#                 sys.exit(1)


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