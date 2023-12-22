"""
Utilities to support transferring data to and from XNAT

matthew thompson
12.21.2023

"""

import logging
from datetime import datetime

def logging_setup(log_level: int, logs_dir: str, name: str) -> None:
    logging.basicConfig(level=log_level, 
                        format='%(asctime)s %(levelname)s: %(message)s',
                        handlers=[
                            logging.FileHandler(f'{logs_dir}/{name}_xnat_{datetime.now()}.log', mode='w'),
                        ])