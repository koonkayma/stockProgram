#!/bin/bash

# Script Name: script_name.sh
# Description: Brief description of the script's purpose

# Author: Your Name
# Date: Date of creation

# Usage: ./script_name.sh [options]

# Options:
#   -h, --help: Display this help message
#   -v, --verbose: Enable verbose output

# Example usage:
#   ./script_name.sh -v

python3 -m venv myenv && source myenv/bin/activate && pip install pandas

function run_import {
  service_name="$1"
  echo "Processing service: $service_name"

  python3 importSECData_AllForms.py --data-dir /media/devmon/Kay/us_data/"$service_name" --log-file "$service_name".log

}

services=(
"2018q3"
"2018q2"
"2018q1"
"2017q4"
"2017q3"
"2017q2"
"2017q1"
"2016q4"
"2016q3"
"2016q2"
"2016q1"
"2015q4"
"2015q3"
"2015q2"
"2015q1"
"2014q4"
"2014q3"
"2014q2"
"2014q1"
"2013q4"
"2013q3"
"2013q2"
"2013q1"
"2012q4"
"2012q3"
"2012q2"
"2012q1"
)

echo "start"

for service in "${services[@]}"; do
  run_import "$service"
done

echo "end..."

# Exit with a specific status code
exit 0
