"""
This module provides functions for cleaning data files, primarily focusing on CSV files.

It includes utilities for:
- Sanitizing column names to be database-friendly.
- Cleaning individual CSV files by converting encoding, standardizing newlines within cells,
  and sanitizing headers.
- Processing a folder of files, applying specific cleaning routines based on file type.
"""
import os
import shutil # For copying files
import csv # For CSV processing
import re # For sanitizing column names
from pathlib import Path
from tqdm import tqdm # For progress bar
from typing import List, Optional, Set

def _sanitize_column_name(name: str) -> str:
    """
    Sanitizes a string to be a valid column name based on a specific sequence of operations.
    - Strips leading/trailing whitespace.
    - Converts to lowercase.
    - Replaces spaces with underscores.
    - Replaces hyphens with underscores.
    - Removes any characters that are NOT alphanumeric or underscore (\W+).
      NOTE: The regex r'\W+' removes all non-alphanumeric characters, which *includes*
      underscores. If underscores introduced by previous steps or originally present
      should be kept, a regex like r'[^a-zA-Z0-9_]+' (to remove characters NOT in
      the set of alphanumerics and underscore) would be more appropriate for that specific step.
      The current implementation follows the provided sequence.
    - Prefixes with 'col_' if it starts with a digit or is empty/invalid after sanitization.
    - Truncates to a reasonable length (e.g., 63 for PostgreSQL compatibility).
    """
    name = str(name).strip() # 1. Strip whitespace
    name = name.lower()      # 2. Convert to lowercase
    name = name.replace(" ", "_") # 3. Replace spaces with underscore
    name = name.replace("-", "_") # 4. Replace hyphens with underscore
    name = re.sub(r'\W+', '', name) # 5. Remove non-alphanumeric (including underscores!)

    if not name or name[0].isdigit(): # Check if name is empty or starts with digit AFTER sanitization
        name = "col_" + (name if name else "unnamed")
        
    return name[:63] # Max length for PostgreSQL identifiers


def clean_csv_file(input_path: Path, output_path: Path, cleaned_output_dir: Path):
    """
    Cleans a single CSV file:
    - Reads with 'cp1252' encoding.
    - Writes the output file with 'utf-8' encoding.
    - Strips whitespace from each cell.
    - Replaces \r\n, \n, \r with ' ' within each cell.
    - Sanitizes header column names.
    - Skips entirely blank rows.

    Args:
        input_path: `Path` object for the input CSV file.
        output_path: `Path` object for the cleaned output CSV file.
        cleaned_output_dir: `Path` object of the directory where cleaned files are stored
                            (used for consistent logging, though not directly for file operations here).
    """
    os.makedirs(output_path.parent, exist_ok=True) # Ensure output directory exists
    # Open input file with 'cp1252' and output file with 'utf-8'.
    # newline='' is important for the csv module to handle line endings correctly.
    with open(input_path, 'r', encoding='cp1252', newline='') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        try:
            # Read the first row, potentially skipping empty leading rows
            header = None
            for i in range(5): # Try up to 5 times to find a non-empty header row
                first_row_candidate = next(reader, None) # Use default to avoid StopIteration here
                if first_row_candidate and any(cell.strip() for cell in first_row_candidate):
                    header = first_row_candidate
                    break
            
            if header:
                sanitized_header_temp = [_sanitize_column_name(col) for col in header]
                # Ensure unique column names after sanitization
                final_header = []
                seen_cols = set()
                for col_name in sanitized_header_temp:
                    unique_col_name = col_name
                    counter = 1
                    while unique_col_name in seen_cols:
                        unique_col_name = f"{col_name}_{counter}"
                        counter += 1
                    final_header.append(unique_col_name)
                    seen_cols.add(unique_col_name)
                writer.writerow(final_header)
            else:
                print(f"      [!] Warning: CSV file '{input_path.name}' appears to have no valid header row after checking initial lines.")
                # If no header, we might still want to process rows if any, or just stop.

            for row in reader:
                if any(cell.strip() for cell in row): # Only write row if at least one cell has non-whitespace content
                    cleaned_row = [str(cell).strip().replace('\r\n',' ').replace('\n',' ').replace('\r',' ') for cell in row]
                    writer.writerow(cleaned_row)
        except StopIteration: # Handles empty CSV files
            print(f"      [!] Warning: CSV file '{input_path.name}' is empty or has no header.")
    print(f"      [✓] Cleaned CSV: '{output_path.name}' using cp1252 -> utf-8 and cell cleaning.")

def process_files_for_cleaning(
    input_folder_path: str,
    output_parent_folder_path: str,
    target_extensions: Optional[Set[str]] = None,
    text_file_extensions: Optional[Set[str]] = None
) -> List[str]:
    """
    Scans files from the input_folder_path, ensures text files are UTF-8 encoded,
    applies specific cleaning routines (like `clean_csv_file` for CSVs),
    and copies/saves the processed files to a 'cleaned_data' subfolder within
    `output_parent_folder_path`.

    Filenames from `input_folder_path` are preserved in the 'cleaned_data' folder,
    as they are assumed to have been pre-sanitized by a previous step (e.g., by `extract_data.py`).

    Args:
        input_folder_path: Path to the folder containing files to process (e.g., "extracted_data").
        output_parent_folder_path: Path to the directory where the 'cleaned_data'
                                   subfolder will be created (e.g., the main source data folder).
        target_extensions: Set of file extensions to process.
                           Defaults to {'.csv', '.xlsx', '.xls', '.txt'}.
        text_file_extensions: Set of extensions (subset of `target_extensions`) to be treated as text files for

                              UTF-8 encoding checks. Defaults to {'.csv', '.txt'}.

    Returns:
        A list of absolute paths to the processed files in the 'cleaned_data' folder.
    """
    if target_extensions is None:
        target_extensions = {'.csv', '.xlsx', '.xls', '.txt'}
    if text_file_extensions is None:
        text_file_extensions = {'.csv', '.txt'}

    processed_target_extensions = {ext.lower() if ext.startswith('.') else f".{ext.lower()}" for ext in target_extensions}
    processed_text_extensions = {ext.lower() if ext.startswith('.') else f".{ext.lower()}" for ext in text_file_extensions}

    input_path = Path(input_folder_path).resolve()
    if not input_path.is_dir():
        print(f"Error: Input folder '{input_folder_path}' does not exist or is not a directory.")
        return []

    output_parent_path = Path(output_parent_folder_path).resolve()
    cleaned_output_dir = output_parent_path / "cleaned_data"
    
    try:
        cleaned_output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Ensured 'cleaned_data' directory exists at: {cleaned_output_dir}")
    except Exception as e:
        print(f"Error creating 'cleaned_data' directory at '{cleaned_output_dir}': {e}")
        return []

    processed_file_paths: List[str] = []
    files_to_process = [f for f in input_path.iterdir() if f.is_file() and f.suffix.lower() in processed_target_extensions]

    print(f"\n--- Starting Cleaning Process ---")
    print(f"Processing {len(files_to_process)} files from '{input_path.name}' into '{cleaned_output_dir.name}'...")

    for original_file_path in tqdm(files_to_process, desc="Cleaning files", unit="file", ncols=100):
        target_filename = original_file_path.name # Assumes names in input_folder are already sanitized
        file_extension = original_file_path.suffix.lower()
        target_disk_path = cleaned_output_dir / target_filename
        
        # tqdm will show progress, print(f"  Processing '{original_file_path.name}' -> '{target_disk_path}'") can be removed if too verbose

        try:
            if file_extension == '.csv':
                clean_csv_file(original_file_path, target_disk_path, cleaned_output_dir)
                processed_file_paths.append(str(target_disk_path.resolve()))
            elif file_extension in processed_text_extensions: # For other text files like .txt
                content = None
                read_encoding = None
                try: # Try UTF-8 first
                    with open(original_file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    read_encoding = 'utf-8'
                    print(f"    '{original_file_path.name}' successfully read as UTF-8.")
                except UnicodeDecodeError:
                    print(f"    '{original_file_path.name}' is not UTF-8. Attempting other encodings...")
                    fallback_encodings = ['cp1252', 'latin-1', 'iso-8859-15']
                    for enc in fallback_encodings:
                        try:
                            with open(original_file_path, 'r', encoding=enc) as f:
                                content = f.read()
                            read_encoding = enc
                            print(f"      Successfully read with '{enc}'.")
                            break 
                        except UnicodeDecodeError:
                            print(f"      Failed to read with '{enc}'.")
                            continue
                        except Exception as e_read:
                            print(f"      Error reading '{original_file_path.name}' with '{enc}': {e_read}")
                            break 
                
                if content is not None:
                    # For non-CSV text files, just convert encoding, no global newline replacement here
                    # as clean_csv_file handles newlines specifically for CSVs at cell level.
                    with open(target_disk_path, 'w', encoding='utf-8') as f_out:
                        f_out.write(content)
                    print(f"      Successfully wrote '{target_filename}' as UTF-8 (read as {read_encoding}).")
                    processed_file_paths.append(str(target_disk_path.resolve()))
                else:
                    print(f"      Error: Could not decode '{original_file_path.name}' with attempted encodings. Copying as is.")
                    shutil.copy2(original_file_path, target_disk_path)
                    processed_file_paths.append(str(target_disk_path.resolve()))
            else: # Binary files or non-text specified files (e.g., .xlsx, .xls)
                shutil.copy2(original_file_path, target_disk_path)
                print(f"    Copied binary/non-specified file '{original_file_path.name}'.")
                processed_file_paths.append(str(target_disk_path.resolve()))
        except Exception as e:
            print(f"    Error processing file '{original_file_path.name}': {e}")

    unique_paths = sorted(list(set(processed_file_paths)))
    print(f"\nTotal files processed and saved to '{cleaned_output_dir.name}': {len(unique_paths)}")
    return unique_paths