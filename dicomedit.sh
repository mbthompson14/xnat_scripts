#!/bin/bash
# Script to exercise the CLI

mkdir -p ./tmp/de6
echo 'version "6.3"' > ./tmp/de6/cli.das
echo '(0008,1030) ?= "sync_test"' >> ./tmp/de6/cli.das

# dcmdump /Users/matthew/Repos/radco/RawDICOM_test/RADCO/RAD22_COL_AE100-144A-01/RAD22_COL_AE100-144A-01/scans/1-Localizer_01/resources/DICOM/files/MR.1.3.12.2.1107.5.2.43.166010.2023041715240430772074961.dcm | grep 0008,1030

# java -jar ~/Downloads/dicom-edit6-6.3.1-jar-with-dependencies.jar \
java -jar /Users/matthew/dicom-edit6-6.5.1-jar-with-dependencies.jar \
   -s ./tmp/de6/cli.das \
   -i /Users/matthew/Repos/radco/RawDICOM_test/RADCO/RAD22_COL_AE100-144A-01/RAD22_COL_AE100-144A-01/scans/1-Localizer_01/resources/DICOM/files/MR.1.3.12.2.1107.5.2.43.166010.2023041715240430772074961.dcm \
   -o ./tmp/de6/cli_test

# dcmdump /tmp/de6/cli_test/MR.1.3.12.2.1107.5.2.43.166010.2023041715240430772074961.dcm | grep 0008,1030


