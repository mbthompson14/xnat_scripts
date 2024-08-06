#!/nyx3/data/RAD22/Scripts/DataOrg/xnat_scripts/env/bin/python3

"""
returns a list of sessions from an XNAT database

matthew thompson
2024.08

"""

import xnat
import argparse
from util import logging_setup, config, connect_to_xnat

CONFIG = 'test_config.json'

def main():
    # get args
    args = parse_args()

    # get server, exp dir
    server, exp_dir = config(CONFIG)

    # setup logging
    logging_setup(log_level=args.debug, logs_dir=f"{exp_dir}/logs", name='listXNATdata')

    # connect to server
    session = connect_to_xnat(server, args.database, exp_dir)

    # call list function

def parse_args():
    parser = argparse.ArgumentParser(
        prog='listXNATdata.py',
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