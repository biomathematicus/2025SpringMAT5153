"""
This module provides functions for finding and extracting data files from a specified
folder. It can handle loose files within the folder (and its subdirectories)
and also extract files from ZIP archives found in the top-level of the folder.

The primary function, `find_and_extract_data_files`, aims to consolidate relevant
data files into a designated "extracted_data" subfolder with sanitized and unique filenames.
"""
import os
import zipfile # For handling .zip files
import shutil # For copying files
import re # For sanitizing names
from pathlib import Path
from tqdm import tqdm # For progress bar
from typing import List, Optional, Set

# Helper function to recursively scan a directory for specified data files
def _scan_directory_recursively_for_data_files(
    scan_path: Path,
    file_extensions: Set[str], # e.g., {'.xlsx', '.csv', '.xls'}
    exclude_dirs: Optional[List[Path]] = None
) -> List[str]:
    """
    Recursively scans a directory for files with specified extensions.

    Args:
        scan_path: The `Path` object of the directory to scan.
        file_extensions: A set of lowercase file extensions (e.g., {'.csv', '.xlsx'}) to look for.
        exclude_dirs: An optional list of `Path` objects representing directories to exclude from the scan.

    Returns:
        A list of strings, where each string is the full absolute path to a found file.
    """
    found_files: List[str] = []
    if not scan_path.is_dir():
        print(f"Warning: '{scan_path}' is not a directory or not accessible. Skipping scan of this path.")
        return []

    if exclude_dirs is None:
        exclude_dirs = []

    try:
        for item in scan_path.rglob('*'):  # rglob('*') iterates through all items recursively
            is_excluded = False
            for excluded_dir_path in exclude_dirs:
                try: # Check if the current item is within one of the excluded directories
                    item.relative_to(excluded_dir_path)
                    is_excluded = True
                    break
                except ValueError:
                    continue
            
            if is_excluded:
                continue

            if item.is_file() and item.suffix.lower() in file_extensions:
                # Check if it's a file and has a target extension 
                found_files.append(str(item.resolve()))
    except PermissionError:
        print(f"Warning: Permission denied while scanning directory '{scan_path}'. Some files might be missed.")
    except Exception as e:
        print(f"Warning: An error occurred while scanning directory '{scan_path}': {e}. Some files might be missed.")
    return found_files

def _sanitize_and_shorten_filename(base_name: str, original_extension: str, max_len_base: int = 50) -> str:
    """
    Sanitizes a base filename string, shortens it if necessary, and appends the original extension.

    The sanitization process includes:
    - Converting to lowercase.
    - Replacing spaces, hyphens, and multiple dots with a single underscore.
    - Removing characters that are not alphanumeric or underscores.
    - Consolidating multiple underscores and stripping leading/trailing ones.

    Args:
        base_name: The base string to use for the filename (without extension).
        original_extension: The original file extension (e.g., ".csv", ".xlsx").
        max_len_base: The maximum allowed length for the sanitized base name part before adding the extension.

    Returns:
        A sanitized and potentially shortened filename string with its original extension.
    """
    # Lowercase and replace common separators/problematic chars
    sanitized = base_name.lower()
    sanitized = re.sub(r'\s+|-+|\.+', '_', sanitized) # Replace spaces, hyphens, multiple dots with underscore
    sanitized = re.sub(r'[^\w_]', '', sanitized)     # Remove non-alphanumeric (excluding underscore)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_') # Consolidate multiple underscores and strip leading/trailing

    if not sanitized: # If sanitization results in an empty string
        sanitized = "unnamed_file"
        
    # Shorten the base name part if too long
    if len(sanitized) > max_len_base:
        sanitized = sanitized[:max_len_base]
        # Ensure it doesn't end with an underscore after truncation
        sanitized = sanitized.strip('_') 

    return f"{sanitized}{original_extension.lower()}"

def _generate_unique_filename(directory: Path, base_filename: str, original_extension: str) -> Path:
    """Generates a unique filename within the specified directory.

    If a file with the `base_filename` (which should already include its extension)
    already exists, a counter (e.g., "_1", "_2") is appended to the stem of the
    filename until a unique name is found.

    Args:
        directory: The `Path` object of the directory where the file will be saved.
        base_filename: The desired filename, including its extension (e.g., "report.csv").
        original_extension: This argument is currently unused as `base_filename` is expected to have the extension.
                            It's kept for potential future refactoring.

    Returns:
        A `Path` object representing the unique file path within the directory.
"""
    # The base_filename here is already sanitized and has extension
    prospective_name = base_filename
    counter = 1
    while (directory / prospective_name).exists():
        name_part, ext_part = os.path.splitext(base_filename)
        prospective_name = f"{name_part}_{counter}{ext_part}"
        counter += 1
    return directory / prospective_name

def find_and_extract_data_files(folder_path: str, target_extensions: Optional[Set[str]] = None) -> List[str]:
    """
    Scans a given folder for files with specified extensions, processes them,
    and copies them to a dedicated "extracted_data" subfolder.

    This function handles:
    1.  Loose files found directly within `folder_path` or its subdirectories (excluding
        the "extracted_data" folder itself). These files are copied to "extracted_data".
    2.  Files within ZIP archives found in the top-level of `folder_path`. Target files
        from these zips are extracted and copied to "extracted_data".

    Filenames in the "extracted_data" folder are sanitized (lowercase, underscores for
    separators, special characters removed), shortened if necessary, and made unique
    to avoid collisions.

    Args:
        folder_path: The path to the folder to scan.
        target_extensions: A set of lower-case file extensions to look for,
                           e.g., {'.xlsx', '.csv', '.txt', '.xls'}.
                           Defaults to {'.xlsx', '.csv', '.xls', '.txt'}.

    Returns:
        A list of strings, where each string is the full absolute path to a file
        that has been copied or extracted into the "extracted_data" subfolder.
        Returns an empty list if the folder doesn't exist, isn't a directory, 
        or no target files are found.
    """
    if target_extensions is None:
        target_extensions = {'.xlsx', '.csv', '.xls', '.txt'}
    
    # Ensure extensions are lowercase and start with a dot
    # Normalize extensions
    processed_extensions = {ext.lower() if ext.startswith('.') else f".{ext.lower()}" for ext in target_extensions}
    print(f"Targeting file extensions: {processed_extensions}")

    try:
        absolute_folder_path = Path(folder_path).resolve(strict=True)
    except FileNotFoundError:
        print(f"Error: The folder '{folder_path}' does not exist.")
        return []
    except Exception as e:
        print(f"Error resolving path '{folder_path}': {e}")
        return []

    if not absolute_folder_path.is_dir():
        print(f"Error: The path '{folder_path}' points to a file, not a folder.")
        return []
    
    # Define the output directory for extracted files
    extracted_data_dir = absolute_folder_path /"extracted_data"
    try:
        extracted_data_dir.mkdir(parents=True, exist_ok=True)
        print(f"Ensured 'extracted_data' directory exists at: {extracted_data_dir.resolve()}")
    except PermissionError:
        print(f"Error: Permission denied to create 'extracted_data' directory at '{extracted_data_dir}'. Cannot extract files from zips.")
    except Exception as e:
        print(f"Error creating 'extracted_data' directory at '{extracted_data_dir}': {e}.")

    all_data_file_paths: List[str] = []

    # --- Stage 1: Process Loose Files ---
    print(f"\nScanning '{absolute_folder_path}' for loose files and copying to '{extracted_data_dir.name}'...")
    loose_original_paths = _scan_directory_recursively_for_data_files(
        absolute_folder_path,
        file_extensions=processed_extensions,
        exclude_dirs=[extracted_data_dir] # Important to exclude the target dir itself
    )

    if not extracted_data_dir.exists() or not extracted_data_dir.is_dir():
        print(f"  Warning: '{extracted_data_dir.name}' directory is not available/valid. Skipping copying of loose files.")
    else:
        if loose_original_paths:
            print(f"  Found {len(loose_original_paths)} loose target file(s) to copy.")
            for original_path_str in tqdm(loose_original_paths, desc="Copying loose files", unit="file", ncols=100):
                original_path = Path(original_path_str)
                try:
                    # Construct a base name for sanitization, including parent directory if in a subfolder
                    original_stem = original_path.stem
                    original_extension = original_path.suffix
                    
                    if original_path.parent != absolute_folder_path:
                        # Include parent dir name if it's in a subfolder for better identification
                        parent_dir_name = original_path.parent.name
                        base_name_for_sanitize = f"{parent_dir_name}_{original_stem}"
                    else:
                        base_name_for_sanitize = original_stem
                    
                    target_filename_stem_ext = _sanitize_and_shorten_filename(base_name_for_sanitize, original_extension)
                    
                    # Generate a unique path in the extracted_data_dir
                    target_disk_path = _generate_unique_filename(extracted_data_dir, target_filename_stem_ext, "") # Extension already part of target_filename_stem_ext
                    
                    # tqdm will show progress, so detailed print can be reduced or removed if too verbose
                    # print(f"    Attempting to copy loose file '{original_path.name}' to '{target_disk_path.name}'")
                    shutil.copy2(original_path, target_disk_path) # copy2 preserves metadata
                    all_data_file_paths.append(str(target_disk_path.resolve()))
                    # print(f"      Successfully copied loose file to: {target_disk_path.resolve()}")
                except Exception as copy_err:
                    print(f"      Error copying loose file '{original_path}' to '{extracted_data_dir}': {copy_err}")
        else:
            print(f"  No loose target files found in '{absolute_folder_path}' (excluding '{extracted_data_dir.name}').")


    
    # --- Stage 2: Process ZIP Archives ---
    print(f"\nChecking for .zip files in the top-level of '{absolute_folder_path}'...")
    
    try:
        top_level_items = list(absolute_folder_path.iterdir())
    except PermissionError:
        print(f"Error: Permission denied to list contents of '{absolute_folder_path}'. Cannot search for zip files.")
        return sorted(list(set(all_data_file_paths))) if all_data_file_paths else []
    except Exception as e:
        print(f"An unexpected error occurred while listing items in '{absolute_folder_path}': {e}")
        return sorted(list(set(all_data_file_paths))) if all_data_file_paths else []

    zip_files_found_in_folder = False
    # Iterate through items in the top-level of the source folder
    for item_path in tqdm(top_level_items, desc="Processing top-level items (zips)", unit="item", ncols=100):
        if item_path.is_file() and item_path.suffix.lower() == '.zip':
            zip_files_found_in_folder = True
            print(f"\nProcessing zip file: '{item_path.name}'...")
            
            if not extracted_data_dir.exists() or not extracted_data_dir.is_dir():
                print(f"  Skipping zip processing for '{item_path.name}' as 'extracted_data' directory is not available/valid.")
                continue

            try:
                with zipfile.ZipFile(item_path, 'r') as zip_ref: # Open the zip file for reading
                    zip_filename_stem = item_path.stem
                    
                    for member_info in zip_ref.infolist(): # Iterate through each member (file/folder) in the zip
                        member_path_obj = Path(member_info.filename)
                        # Check if the member is a file and has one of the target extensions
                        if not member_info.is_dir() and member_path_obj.suffix.lower() in processed_extensions:
                            original_zip_member_stem = member_path_obj.stem
                            original_zip_member_extension = member_path_obj.suffix

                            # Construct a base name for sanitization: zip_filename_stem + original_member_stem
                            base_name_for_sanitize = f"{zip_filename_stem}_{original_zip_member_stem}"
                            
                            target_filename_stem_ext = _sanitize_and_shorten_filename(
                                base_name_for_sanitize, original_zip_member_extension
                            )
                            
                            # Ensure filename is unique in the target directory
                            # Extension already part of target_filename_stem_ext
                            target_disk_path = _generate_unique_filename(extracted_data_dir, target_filename_stem_ext, "")
                            
                            # print(f"  Attempting to extract '{member_info.filename}' from '{item_path.name}' to '{target_disk_path.name}'")
                            
                            try:
                                with zip_ref.open(member_info.filename) as source_file_in_zip, \
                                     open(target_disk_path, "wb") as target_file_on_disk:
                                    target_file_on_disk.write(source_file_in_zip.read())
                                
                                all_data_file_paths.append(str(target_disk_path.resolve()))
                                # print(f"    Successfully extracted and saved as: {target_disk_path.resolve()}")
                            except Exception as extraction_err:
                                print(f"    Error extracting file '{member_info.filename}' from '{item_path.name}': {extraction_err}")
            
            except zipfile.BadZipFile:
                print(f"  Error: '{item_path.name}' is not a valid zip file or is corrupted. Skipping.")
            except PermissionError as pe:
                print(f"  Error: Permission denied during processing of '{item_path.name}': {pe}. Skipping.")
            except Exception as e:
                print(f"  An unexpected error occurred while processing zip file '{item_path.name}': {e}. Skipping.")
    
    if not zip_files_found_in_folder:
        print("No .zip files found in the top-level of the specified folder.")

    # Return a sorted list of unique paths to all processed files
    if all_data_file_paths:
        unique_paths = sorted(list(set(all_data_file_paths)))
        print(f"\nTotal unique target file paths found/created: {len(unique_paths)}")
        return unique_paths
    else:
        print(f"\nNo target files (extensions: {processed_extensions}) were found in '{folder_path}' (or its subdirectories/zip files), or an error prevented access.")
        return []