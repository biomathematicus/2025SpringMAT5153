import os
import zipfile # For handling .zip files
import shutil # For copying files
from pathlib import Path
from typing import List, Optional, Set

from utils.extract_data import find_and_extract_data_files
from utils.clean_data import process_files_for_cleaning
from utils.import_to_sql import sanitize_identifier, import_cleaned_data_to_postgres



if __name__ == "__main__":
    current_script_path = Path(__file__).parent.resolve()
    source_data_folder_for_script = current_script_path/"source_data"
    expected_extracted_data_dir = source_data_folder_for_script / "extracted_data"


    print(f"\n--- Step 1: Finding and Extracting Data Files to '{expected_extracted_data_dir.name}' ---")
    # Default will look for .xlsx, .xls, and .csv
    list_of_data_files = find_and_extract_data_files(str(source_data_folder_for_script))
    
    # Example: To look only for .csv files:
    # list_of_data_files = find_and_extract_data_files(str(source_data_folder_for_script), target_extensions={'.csv'})
    # list_of_data_files = find_and_extract_data_files(str(source_data_folder_for_script), target_extensions={'.xlsx'})
    # list_of_data_files = find_and_extract_data_files(str(source_data_folder_for_script), target_extensions={'.xls'})

    if list_of_data_files:
        print("\nFound/Created the following target files (full paths):")
        for file_path_str in list_of_data_files:
            file_p = Path(file_path_str)
            print(file_p.resolve())
            # ALL files in the list should now be in the extracted_data_dir
            assert file_p.parent.resolve() == expected_extracted_data_dir.resolve(), \
                   f"File '{file_p.name}' is not in the expected 'extracted_data' directory. Found in: '{file_p.parent.resolve()}'"
    else:
        print(f"\nNo target files were found or created from '{source_data_folder_for_script.resolve()}'.")

    print(f"\n--- Verifying contents of '{expected_extracted_data_dir.name}' directory ---")
    if expected_extracted_data_dir.exists() and expected_extracted_data_dir.is_dir():
        print(f"Contents of '{expected_extracted_data_dir}':")
        extracted_count = 0
        for item in expected_extracted_data_dir.iterdir():
            print(f"  - {item.name}")
            extracted_count +=1
        if extracted_count == 0:
            print("  (empty)")
    else:
        print(f"'extracted_data' directory not found at '{expected_extracted_data_dir}' or is not a directory.")
    
    # --- Step 2: Process files from 'extracted_data' to 'cleaned_data' ---
    if expected_extracted_data_dir.exists() and expected_extracted_data_dir.is_dir():
        cleaned_files_list = process_files_for_cleaning(
            input_folder_path=str(expected_extracted_data_dir),
            output_parent_folder_path=str(source_data_folder_for_script) # 'cleaned_data' will be created here
        )
        
        expected_cleaned_data_dir = source_data_folder_for_script / "cleaned_data"
        if cleaned_files_list:
            print(f"\n--- Verifying contents of '{expected_cleaned_data_dir.name}' directory ---")
            if expected_cleaned_data_dir.exists() and expected_cleaned_data_dir.is_dir():
                print(f"Contents of '{expected_cleaned_data_dir}':")
                cleaned_count = 0
                for item_path_str in cleaned_files_list:
                    item = Path(item_path_str)
                    print(f"  - {item.name} (is in cleaned_data: {item.parent.name == 'cleaned_data'})")
                     # Verify encoding for a known converted file
                    if item.name == "latin1_text.txt":
                        try:
                            with open(item, 'r', encoding='utf-8') as f_check:
                                f_check.read()
                            print(f"    Successfully verified '{item.name}' is UTF-8 in cleaned_data.")
                        except UnicodeDecodeError:
                            print(f"    ERROR: '{item.name}' in cleaned_data is NOT UTF-8.")
            else:
                print(f"'{expected_cleaned_data_dir.name}' directory not found or is not a directory after processing.")
    else:
        print(f"\nSkipping cleaning process as '{expected_extracted_data_dir.name}' does not exist or is not a directory.")

    # --- Step 3: Import data from 'cleaned_data' to PostgreSQL ---
    host = os.getenv("PostgreSQL_HOST")
    port = os.getenv("PostgreSQL_PORT")
    dbname = os.getenv("PostgreSQL_DBNAME")
    user = os.getenv("PostgreSQL_USER")
    password = os.getenv("PostgreSQL_PWD")
    target_schema = os.getenv("POSTGRESQL_SCHEMA", "CRDCdata") # Default to 'my_data_schema' if env var not set


    expected_cleaned_data_dir = source_data_folder_for_script / "cleaned_data"
    if expected_cleaned_data_dir.exists() and expected_cleaned_data_dir.is_dir():
        # !!! IMPORTANT: Configure your PostgreSQL connection details below !!!
        db_connection_params = {
            "host": host,        # Or your DB host
            "port": port,             # Or your DB port
            "dbname": dbname, # Replace with your database name
            "user": user,    # Replace with your PostgreSQL username
            "password": password # Replace with your PostgreSQL password
        }
        print(f"\n--- Attempting to import files from '{expected_cleaned_data_dir.name}' to PostgreSQL schema '{target_schema}' ---")
        print(f"Ensure your PostgreSQL server is running, the database '{db_connection_params['dbname']}' exists, and you have permissions on schema '{target_schema}'.")
        
        import_results = import_cleaned_data_to_postgres(
            cleaned_data_folder_path=str(expected_cleaned_data_dir),
            db_params=db_connection_params,
            schema_name=target_schema
        )
        print("\n--- PostgreSQL Import Summary ---")
        successful_imports = 0
        failed_imports = 0
        for filename, status in import_results.items():
            print(f"  {filename}: {status}")
            if "Successfully imported" in status:
                successful_imports += 1
            elif "error_summary" not in filename and "unexpected_error_summary" not in filename and "database_error_summary" not in filename and "summary" not in filename : # Count actual file import failures
                failed_imports += 1
        print(f"\n--- Import Totals ---")
        print(f"  Successfully imported files: {successful_imports}")
        print(f"  Failed file imports: {failed_imports}")
    else:
        print(f"\nSkipping PostgreSQL import as '{expected_cleaned_data_dir.name}' does not exist or is not a directory.")