
import os
import csv
from datetime import datetime
import re # For checking folder pattern

# --- Configuration ---
BASE_DATA_DIR = "/media/devmon/Kay/My Documents/workspace/Stock/us_data/"
TARGET_CIK = 1652044 # Example CIK (Meta/Facebook) - CHANGE AS NEEDED
OUTPUT_CSV_FILE = f"sec_numeric_extract_cik_{TARGET_CIK}.csv"
FOLDER_PATTERN = re.compile(r"^\d{4}q[1-4]$") # Regex to match YYYYqN pattern

# --- Helper Functions ---
def format_date_sec(date_str):
    """Converts YYYYMMDD string to YYYY-MM-DD format."""
    if not date_str or len(date_str) != 8:
        return None
    try:
        return datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')
    except ValueError:
        return None # Handle invalid date formats

def safe_int(value):
    """Safely convert to integer, returning None on failure."""
    if value is None or value == '':
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None

def safe_decimal(value):
    """Safely convert to float (for CSV), returning None on failure."""
    # Databases handle DECIMAL, float is usually fine for CSV intermediate
    if value is None or value == '':
        return None
    try:
        # You might want to use Decimal type if extreme precision is needed
        # from decimal import Decimal, InvalidOperation
        # return Decimal(value)
        return float(value)
    except (ValueError, TypeError):
        return None

# --- Main Script ---

print(f"Starting data extraction for CIK: {TARGET_CIK}")
print(f"Base directory: {BASE_DATA_DIR}")

# 1. List potential quarterly folders
all_items = os.listdir(BASE_DATA_DIR)
quarterly_folders = sorted([
    os.path.join(BASE_DATA_DIR, item)
    for item in all_items
    if os.path.isdir(os.path.join(BASE_DATA_DIR, item)) and FOLDER_PATTERN.match(item)
])

print("\nFound Quarterly Folders:")
if not quarterly_folders:
    print("  No folders matching pattern YYYYqN found. Exiting.")
    exit()

for i, folder in enumerate(quarterly_folders):
    print(f"  [{i}] {os.path.basename(folder)}")

# *** MODIFICATION POINT: Select folders ***
# By default, process all found folders.
# To process specific folders, modify the `selected_folders` list below.
# Example: Process only the first and third folder:
# selected_folders = [quarterly_folders[0], quarterly_folders[2]]
# Example: Process all folders:
selected_folders = quarterly_folders
# Example: Process only 2024q3 if it exists:
# selected_folders = [f for f in quarterly_folders if os.path.basename(f) == '2024q3']

print(f"\nSelected folders to process: {[os.path.basename(f) for f in selected_folders]}")

all_extracted_data = []
processed_adsh = set() # Keep track of adsh already processed from sub.txt

# 2. & 3. Read files and process data
for folder_path in selected_folders:
    folder_name = os.path.basename(folder_path)
    print(f"\nProcessing folder: {folder_name}...")

    sub_file_path = os.path.join(folder_path, "sub.txt")
    num_file_path = os.path.join(folder_path, "num.txt")

    # Check if files exist
    if not os.path.exists(sub_file_path):
        print(f"  WARNING: sub.txt not found in {folder_name}. Skipping folder.")
        continue
    if not os.path.exists(num_file_path):
        print(f"  WARNING: num.txt not found in {folder_name}. Skipping folder.")
        continue

    # Read relevant submission data for the target CIK
    print(f"  Reading {sub_file_path}...")
    sub_data_for_cik = {} # Store adsh -> {metadata} for target CIK in this folder
    try:
        with open(sub_file_path, 'r', encoding='utf-8') as f_sub:
            reader = csv.reader(f_sub, delimiter='\t')
            header_sub = next(reader) # Read header
            # Find column indices (robust to order changes)
            try:
                idx_sub = {name: header_sub.index(name) for name in ['adsh', 'cik', 'form', 'period', 'fy', 'fp']}
            except ValueError as e:
                print(f"  ERROR: Missing required column in {sub_file_path}: {e}. Skipping folder.")
                continue

            for row in reader:
                try:
                    row_cik = safe_int(row[idx_sub['cik']])
                    if row_cik == TARGET_CIK:
                        adsh = row[idx_sub['adsh']]
                        # Only store if not already seen (in case CIK has multiple filings in a quarter processed earlier)
                        # Although typically one sub.txt per quarter folder.
                        if adsh not in processed_adsh:
                             sub_data_for_cik[adsh] = {
                                'cik': TARGET_CIK, # Already checked
                                'form': row[idx_sub['form']] or None,
                                'period': format_date_sec(row[idx_sub['period']]),
                                'fy': safe_int(row[idx_sub['fy']]),
                                'fp': row[idx_sub['fp']] or None
                             }
                             processed_adsh.add(adsh) # Mark as processed globally
                except IndexError:
                    print(f"  WARNING: Skipping malformed row in {sub_file_path}: {row}")
                    continue # Skip malformed rows
    except Exception as e:
        print(f"  ERROR reading {sub_file_path}: {e}. Skipping folder.")
        continue

    if not sub_data_for_cik:
        print(f"  No submissions found for CIK {TARGET_CIK} in {sub_file_path}.")
        # Continue to the next folder, no need to read num.txt if no relevant ADSH
        continue
    else:
         print(f"  Found {len(sub_data_for_cik)} relevant submission(s) for CIK {TARGET_CIK}.")


    # Read numeric data and filter by ADSH found above
    print(f"  Reading {num_file_path}...")
    rows_added_from_folder = 0
    try:
        with open(num_file_path, 'r', encoding='utf-8') as f_num:
            reader = csv.reader(f_num, delimiter='\t')
            header_num = next(reader)
            try:
                idx_num = {name: header_num.index(name) for name in ['adsh', 'tag', 'version', 'coreg', 'ddate', 'qtrs', 'uom', 'value', 'footnote']}
            except ValueError as e:
                print(f"  ERROR: Missing required column in {num_file_path}: {e}. Skipping associated NUM data.")
                continue # Skip processing this num file if header is wrong

            for row in reader:
                try:
                    adsh = row[idx_num['adsh']]
                    # Check if this adsh belongs to our target CIK's submissions
                    if adsh in sub_data_for_cik:
                        submission_context = sub_data_for_cik[adsh]

                        # Extract and format data according to sec_numeric_data structure
                        output_row = {
                            'adsh': adsh,
                            'tag': row[idx_num['tag']],
                            'version': row[idx_num['version']],
                            'ddate': format_date_sec(row[idx_num['ddate']]),
                            'qtrs': safe_int(row[idx_num['qtrs']]),
                            'uom': row[idx_num['uom']],
                            'value': safe_decimal(row[idx_num['value']]),
                            'coreg': row[idx_num['coreg']] or None, # Handle empty string as NULL
                            'footnote': row[idx_num['footnote']] or None, # Handle empty string as NULL
                            'cik': submission_context['cik'],
                            'form': submission_context['form'],
                            'period': submission_context['period'],
                            'fy': submission_context['fy'],
                            'fp': submission_context['fp'],
                            # 'imported_at': None, # These are DB generated
                            # 'updated_at': None,  # These are DB generated
                        }
                        # Basic validation: Check required fields after formatting
                        if output_row['adsh'] and output_row['tag'] and output_row['version'] \
                           and output_row['ddate'] is not None and output_row['qtrs'] is not None \
                           and output_row['uom'] and output_row['cik'] is not None:
                             all_extracted_data.append(output_row)
                             rows_added_from_folder += 1
                        else:
                            print(f"  WARNING: Skipping row due to missing required fields (post-format): adsh={output_row['adsh']}, tag={output_row['tag']}, version={output_row['version']}, ddate={output_row['ddate']}, qtrs={output_row['qtrs']}, uom={output_row['uom']}, cik={output_row['cik']}")

                except IndexError:
                     print(f"  WARNING: Skipping malformed row in {num_file_path}: {row}")
                     continue # Skip malformed rows
    except Exception as e:
        print(f"  ERROR reading {num_file_path}: {e}. Skipping associated NUM data.")
        continue

    print(f"  Added {rows_added_from_folder} data rows from this folder.")


# 4. Output results to CSV
print(f"\nTotal data rows extracted: {len(all_extracted_data)}")

if all_extracted_data:
    print(f"Writing data to {OUTPUT_CSV_FILE}...")
    # Define the exact header order matching the target table structure
    fieldnames = [
        'adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom', 'value',
        'coreg', 'footnote', 'cik', 'form', 'period', 'fy', 'fp'
        # Excluding imported_at, updated_at as they are DB managed
    ]
    try:
        with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_extracted_data)
        print("Successfully wrote CSV file.")
    except Exception as e:
        print(f"ERROR writing CSV file: {e}")
else:
    print("No data extracted for the specified CIK and folders. CSV file not created.")

print("\nScript finished.")


# --- Suggestions for Checking Missing Data ---

print("""
--- Suggestions for Checking Missing Data (Beyond this script) ---

1.  **Database Count vs. Script Count:**
    *   Run this script for a specific CIK and date range (by selecting folders).
    *   Run a SQL query against your `sec_numeric_data` table:
        ```sql
        SELECT COUNT(*)
        FROM nextcloud.sec_numeric_data
        WHERE cik = {TARGET_CIK}
          AND ddate BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'; -- Adjust date range based on processed folders
        ```
    *   Compare the counts. A mismatch indicates potential issues (either script error or import error).

2.  **Record-Level Comparison (More Thorough):**
    *   Load the CSV generated by this script into a temporary database table or a pandas DataFrame.
    *   Query your main `sec_numeric_data` table for the same CIK and date range, loading it into another DataFrame.
    *   Define the primary key columns: `['adsh', 'tag', 'version', 'ddate', 'qtrs', 'uom']`.
    *   Use pandas `merge` with `indicator=True` on the primary key columns to find rows present in one source but not the other:
        ```python
        # import pandas as pd
        # df_script = pd.read_csv(OUTPUT_CSV_FILE, ...)
        # df_db = pd.read_sql("SELECT * FROM ... WHERE cik = ...", db_connection)
        # # Ensure data types match, especially dates and numbers
        # comparison = pd.merge(df_script, df_db, on=primary_key_columns, how='outer', indicator=True, suffixes=('_script', '_db'))
        # missing_in_db = comparison[comparison['_merge'] == 'left_only']
        # missing_in_script = comparison[comparison['_merge'] == 'right_only']
        # print("Rows in script output but not in DB:", len(missing_in_db))
        # print("Rows in DB but not in script output:", len(missing_in_script))
        ```
    *   This helps pinpoint exact missing records. You can also compare the `value` column for rows that *do* match on the primary key to check for data discrepancies.

3.  **Targeted Spot Checks:**
    *   Pick a few specific filings (`adsh`) for the CIK known to be in the processed folders.
    *   Manually query your database for data points from that filing:
        ```sql
        SELECT tag, ddate, qtrs, uom, value
        FROM nextcloud.sec_numeric_data
        WHERE adsh = 'specific_adsh_value'
        ORDER BY tag, ddate;
        ```
    *   Compare this against the data for the same `adsh` in the `num.txt` file (you can manually inspect the file or adapt the script to output data for just one `adsh`).

4.  **Import Process Logging:**
    *   Ensure your actual data import process (whatever tool or script you use) has robust logging. It should log:
        *   Which files it processes.
        *   How many rows it reads from each file.
        *   How many rows it successfully inserts/updates.
        *   Any errors encountered (file not found, data type issues, constraint violations).
    *   Reviewing these logs is often the first step in diagnosing import problems.

This script provides the raw extraction; comparing its output to your database using methods #1 or #2 is the most direct way to check if your import process missed data that *should* have been loaded according to the source files.
""")