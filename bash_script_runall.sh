#!/bin/zsh

# This is the main script that will be run to process the entire ingestion pipeline.

# Creating the variables to produce the location/file that is needed to determine if the tests passed or failed.
TODAYS_DATE=$(date +"%Y-%m-%d") 
FILE_NAME=$(find logs/testing -name "${TODAYS_DATE}*")

# Calling the script to run through the unittest processes.
python -m test -v

# The command to pull the result of the testing suite into a variable to be used by the logical operators.
TEST_RESULTS=$(tail -1 ${FILE_NAME} | cut -d " " -f 1)

# The logical flow to determine if the last line of the testing log has the OK signal to continue with the data processing.
# If the FAILED signal is active, we will print messages to the screen and exit the script. 
if [ "$TEST_RESULTS" = "OK" ]
then
  python -m python_script_runall
else
  echo "WARNING: THE TESTING SUITE DISCOVERED AN ERROR WITHIN THE DATA INGESTION PROCESS."
  echo "PLEASE INVESTIGATE THE FOLLOWING FILE: ${FILE_NAME}"
  exit 1
fi

# Once the scripts are complete, we need to move the sqlite database and the csv file from the dev folder into the prod folder.
cp data_dev/cademycode_analytics.db data_prod/cademycode_analytics.db
cp data_dev/subscriber_data_clean.csv data_prod/subscriber_data_clean.csv

echo "The process finished succesfully".

exit 0
