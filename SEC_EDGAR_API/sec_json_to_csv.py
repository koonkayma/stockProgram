import requests
import csv
import os
import time
import logging

# --- Configuration ---
#CIK = "0001652044" # Google/Alphabet's CIK (ensure leading zeros)
CIK = "0001855612" # GRAB

API_URL = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{CIK}.json"
OUTPUT_CSV_FILE = f"CIK{CIK}_facts.csv"

# --- IMPORTANT: Use your actual User-Agent ---
# Format: Company Name/App Name ContactEmail@example.com
USER_AGENT = "YourCompanyName/DataVerification YourEmail@example.com" # REPLACE

# Check if the User-Agent looks like the placeholder
if "YourCompanyName" in USER_AGENT or "YourEmail@example.com" in USER_AGENT:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!! WARNING: Default User-Agent detected.                         !!")
    print("!! Please REPLACE the USER_AGENT variable in the script          !!")
    print("!! with your actual identification details.                      !!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    time.sleep(3)

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Main Script ---
if __name__ == "__main__":
    logging.info(f"Fetching data for CIK {CIK} from {API_URL}...")
    headers = {'User-Agent': USER_AGENT}
    company_facts_data = None
    all_rows_data = [] # List to hold data for all CSV rows

    try:
        response = requests.get(API_URL, headers=headers, timeout=45) # Increased timeout
        response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
        company_facts_data = response.json()
        logging.info("Successfully fetched and parsed JSON data.")

    except requests.exceptions.Timeout:
        logging.error(f"Error: Request timed out while fetching data from {API_URL}")
        exit(1)
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Error: HTTP error occurred: {http_err} - Status Code: {response.status_code}")
        logging.error(f"Response Text (partial): {response.text[:500]}")
        exit(1)
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error: An error occurred during the request: {req_err}")
        exit(1)
    except requests.exceptions.JSONDecodeError:
        logging.error(f"Error: Failed to decode JSON response from {API_URL}")
        logging.error(f"Response Text (partial): {response.text[:500]}")
        exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        exit(1)

    if not company_facts_data:
        logging.error("Failed to get data. Exiting.")
        exit(1)

    # Extract constant info
    cik_str = str(company_facts_data.get('cik', CIK)).zfill(10) # Ensure 10 digits
    entity_name = company_facts_data.get('entityName', 'N/A')

    logging.info(f"Processing facts for: {entity_name} (CIK: {cik_str})")

    # Define the header row for the CSV
    # Include potentially missing 'frame' key
    header = [
        'cik', 'entity_name', 'taxonomy', 'tag', 'label', 'description', 'unit',
        'end_date', 'value', 'accession_number', 'fiscal_year', 'fiscal_period',
        'form', 'filed_date', 'frame'
    ]

    # Iterate through the nested structure
    facts = company_facts_data.get('facts', {})
    for taxonomy, tags_dict in facts.items():
        for tag, tag_details in tags_dict.items():
            label = tag_details.get('label', '')
            description = tag_details.get('description', '')
            units = tag_details.get('units', {})

            for unit, data_points_list in units.items():
                for data_point in data_points_list:
                    # Prepare data for one row in the CSV
                    row_data = [
                        cik_str,
                        entity_name,
                        taxonomy,
                        tag,
                        label,
                        description,
                        unit,
                        data_point.get('end', ''),   # Period end date
                        data_point.get('val', None), # The actual value
                        data_point.get('accn', ''),  # Accession number
                        data_point.get('fy', None),  # Fiscal year
                        data_point.get('fp', ''),   # Fiscal period (FY, Q1, etc.)
                        data_point.get('form', ''), # Form type (10-K, 10-Q)
                        data_point.get('filed', ''),# Date filed
                        data_point.get('frame', '') # Optional period frame (e.g., CY2023Q1I) - use .get()
                    ]
                    all_rows_data.append(row_data)

    logging.info(f"Processed {len(all_rows_data)} data points.")

    # Write the collected data to a CSV file
    try:
        logging.info(f"Writing data to {OUTPUT_CSV_FILE}...")
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header) # Write the header
            writer.writerows(all_rows_data) # Write all the data rows
        logging.info(f"Successfully created CSV file: {os.path.abspath(OUTPUT_CSV_FILE)}")

    except IOError as io_err:
        logging.error(f"Error writing to CSV file {OUTPUT_CSV_FILE}: {io_err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred during CSV writing: {e}", exc_info=True)