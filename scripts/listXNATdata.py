#!/Users/matthew/Repos/xnat_scripts/venv/bin/python3

# /nox2/data/RAD22/Scripts/DataOrg/xnat_scripts/venv/bin/python3

"""
returns a list of sessions from an XNAT database

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

    # call list function
    list_data(server, args.database)

def list_data(server: str, database: str) -> None:

    logging.info(f'Running {Path(__file__).name}')
    logging.info(f'Database: {database}')

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

                # loop thru and print sessions
                for session in project.experiments.values():
                    print(session.label)
                    logging.info(f'[session]: {session.label}')

            else:
                # could not find project on the server
                logging.error('XNAT database does not exist')
                sys.exit(1)


def parse_args():
    parser = argparse.ArgumentParser(
        prog=Path(__file__).name,
        description="returns a list of sessions from an XNAT database",
        epilog='''
output:
    returns list (one line per session) of MR sessions that are available on the XNAT database specified, to STDOUT
    
    format is [session]
              [session]

example:
    listXNATdata -x RADCO
    
    RAD22_COL_AE100-090Aka_01
    RAD22_COL_AE100-133A_01
    ...

error exit codes:
    0 - no error
    1 - XNAT database does not exist
    9 - XNAT connection error

        ''',
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-x','--database',
        help='name of XNAT database to query'
    )

    parser.add_argument(
        '-b','--binary',
        help='list only those sessions with binary data files',
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