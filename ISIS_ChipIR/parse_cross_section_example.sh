#!/bin/bash
set -e
set -x

# Set scripts path
HOME=/home/fernando
DATA_PATH=${HOME}/temp/ChipIR2021
SCRIPT_PATH=${HOME}/git_research/cross-section-parsers/ISIS_ChipIR
FACTOR=5600000

K40_LOGS=${DATA_PATH}/K40
# Call the first script from the log folder
cd ${K40_LOGS}
rm -rf logs_parsed/

# Run first script
${SCRIPT_PATH}/ISIS_ChipIR/first_parser_sdc-csv-generator.py

# full_neutron_logs.txt is generated using the following command
NEUTRON_LOG=full_neutron_logs.txt
${SCRIPT_PATH}/ISIS_ChipIR/merge_neutrons_count_file.py ${NEUTRON_LOG} ${DATA_PATH}/countlog/countlog-2021*.txt

# calc the cross section
${SCRIPT_PATH}/ISIS_ChipIR/calc_cross_section_2021_may.py ${NEUTRON_LOG}\
                                      ${K40_LOGS}/logs_parsed/logs_parsed_carolk401.csv\
                                      ${FACTOR} <distance factor file>

