import logging
import xnat
from datetime import datetime
HOST = 'https://xnatccn.semel.ucla.edu/'


logger = logging.basicConfig(level=logging.DEBUG, 
                            format='%(asctime)s %(levelname)s: %(message)s',
                            handlers=[
                                logging.FileHandler(f'logs/import_test_{datetime.now()}.log', mode='w'),
                                #logging.StreamHandler(sys.stdout)
                            ])


with xnat.connect(HOST, default_timeout = 600, loglevel=logging.DEBUG, logger=logger) as session:
    session.services.import_dir(directory='/Users/matthew/Repos/radco/RawDICOM_edit/sync_test/RAD22_MGH_BU101-038Aka_01/',project='sync_test')