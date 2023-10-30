import logging
import xnat
HOST = 'https://xnatccn.semel.ucla.edu/'

with xnat.connect(HOST, loglevel=logging.DEBUG) as session:
    session.services.import_dir(directory='/Users/matthew/Repos/radco/RawDICOM_edit/sync_test/RAD22_GSU_ZP20230321_01/',project='sync_test')