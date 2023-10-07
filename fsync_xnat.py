#!/Users/matthew/anaconda3/bin/python
# edit shebang to python path on nyx


##############################################################################################
#
## Sync XNAT ##
#
# This script copies all RADCO dicom files on the XNAT server to the rawDicom folder on nyx3,
# that do not already exist on nyx3, preserving directory structure
# 
# Matthew Thompson - 9/28/23
# mbthompson14@gmail.com
#
# https://xnat.readthedocs.io/en/latest/index.html
#
###############################################################################################
#
# XNAT credentials should be stored in ~/.netrc in the following format:
#
# machine xnatccn.semel.ucla.edu
# login USERNAME
# password PASSWORD
#
###############################################################################################

#user='mthompson',password='z03bRe@d'

import pyxnat

with pyxnat.Interface(server='https://xnatccn.semel.ucla.edu/',user='mthompson',password='z03bR3@d') as session:

    print(session.select.projects().get())