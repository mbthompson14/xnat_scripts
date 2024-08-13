"""

common utilities for:

listXNATdata.py
grabXNATdata.py
pushXNATdata.py

matthew thompson
2024.08

"""
import sys
import xnat
import json
import logging
from datetime import datetime

def config(config_filepath: str) -> tuple[str,str]:

    # read config file
    with open(config_filepath) as f:
        config_json = json.load(f)
        # return server URL & exp dir
        return config_json['xnat_server'], config_json['exp_dir']

def logging_setup(log_level: bool, logs_dir: str, name: str) -> None:
    
    if log_level:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(level=level, 
                        format='%(asctime)s %(levelname)s: %(message)s',
                        handlers=[
                            logging.FileHandler(f'{logs_dir}/{name}_{datetime.now():%Y-%m-%d_%H:%M:%S}.log', mode='w'),
                        ])
    
# def connect_to_xnat(server: str) -> xnat.session.XNATSession:

#     logging.info('attempting connection to XNAT server')

#     try:
#         session = xnat.connect(server=server, default_timeout = 600, loglevel=logging.root.level)
#     except:
#         logging.error('XNAT connection error')
#         sys.exit(9)
#     else:
#         with session:
#             return session

    # with xnat.connect(server=server, default_timeout = 600, loglevel=logging.root.level) as (session, err):
    #     if err:
    #         print(f'XNAT connection error: {err}')
    #         sys.exit(9)
    #     else:
    #         return session