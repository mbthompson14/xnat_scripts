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
from pathlib import Path
from datetime import datetime
from urllib.parse import ParseResult, urlparse

def config(config_filepath: str) -> tuple[str,str]:

    # read config file
    with open(config_filepath) as f:
        # load json
        config_json = json.load(f)

        # parse xnat url, add scheme (https) if missing
        p = urlparse(config_json['xnat_server'])
        netloc = p.netloc or p.path
        path = p.path if p.netloc else ''
        p = ParseResult('https', netloc, path, *p[3:])
        xnat_url = p.geturl()

        # return server URL & exp dir
        return xnat_url, config_json['exp_dir']

def logging_setup(log_level: bool, logs_dir: str, name: str) -> None:
    
    # get logging level
    if log_level:
        level = logging.DEBUG
    else:
        level = logging.INFO

    # set log dir path & create (+ parents) if it doesn't exist
    # e.g. LOGS/XNAT/2024/202408/listXNATdata_20240809_144139.log
    log_path = f'{logs_dir}/{datetime.now():%Y}/{datetime.now():%Y%m}'
    Path(log_path).mkdir(parents=True, exist_ok=True)

    # define logger
    logging.basicConfig(level=level, 
                        format='%(asctime)s %(levelname)s: %(message)s',
                        handlers=[
                            logging.FileHandler(f'{log_path}/{name}_{datetime.now():%Y%m%d_%H%M%S}.log', mode='w'),
                        ])