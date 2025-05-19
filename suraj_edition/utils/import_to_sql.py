import psycopg2 # For PostgreSQL interaction
from psycopg2 import sql # For safe SQL query construction
import csv as csv_module # To avoid conflict with variable named 'csv'
import re # For sanitizing identifiers
from pathlib import Path
import pandas as pd # For reading Excel files
import io # For in-memory CSV handling
from tqdm import tqdm # For progress bar
from typing import Optional, Set, Dict, List

def sanitize_identifier(name: str, is_column: bool = False) -> str:
    """
    Sanitizes a string to be a valid SQL identifier (table or column name).
    - Converts to lowercase.
    - Replaces spaces, hyphens with underscores.
    - Removes characters other than alphanumeric and underscore.
    - Prefixes with 'tbl_' or 'col_' if it starts with a digit or is empty after sanitization.
    - Truncates to 63 characters (PostgreSQL default limit).
    """
    if not is_column: # For table names derived from filenames
        name = Path(name).stem
    
    name = name.strip().lower()
    name = re.sub(r'[\s.-]+', '_', name)  # Replace space, dot, hyphen with underscore
    name = re.sub(r'[^\w_]', '', name)    # Remove non-alphanumeric (excluding underscore)
    name = re.sub(r'_+', '_', name).strip('_') # Consolidate multiple underscores and strip leading/trailing
    
    # If the name is empty after sanitization or starts with a digit
    if not name or name[0].isdigit():
        prefix = "col_" if is_column else "tbl_"
        name = prefix + (name if name else "unnamed") # e.g. tbl_unnamed or tbl_123
        
    return name[:63]

def _get_table_columns(cur, schema_name: str, table_name: str) -> Optional[List[str]]:
    """
    Queries the database to get the column names for a specific table in a schema.
    Returns a list of column names or None if the table does not exist.
    """
    try:
        cur.execute(sql.SQL("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = {} AND table_name = {}
            ORDER BY ordinal_position;
        """).format(sql.Literal(schema_name), sql.Literal(table_name)))
        
        columns = [row[0] for row in cur.fetchall()]
        
        if not columns:
            # Table might not exist, or has no columns (unlikely for a CSV import target)
             print(f"      [!] Warning: Table '{schema_name}.{table_name}' found but has no columns or does not exist according to information_schema.")
             return None # Indicate table structure couldn't be retrieved
             
        return columns
    except Exception as e:
        print(f"      [!] Error querying columns for '{schema_name}.{table_name}': {e}")
        return None # Indicate failure to retrieve columns

def import_cleaned_data_to_postgres(
    cleaned_data_folder_path: str,
    db_params: dict,
    schema_name: str = "public", # Default to public, but can be overridden
    target_extensions: Optional[Set[str]] = None,
) -> Dict[str, str]:
    """
    Imports data from files in the cleaned_data_folder into PostgreSQL.
    Currently supports .csv files. For CSVs, it creates/replaces a table
    based on the CSV header and uses the COPY command.

    Args:
        cleaned_data_folder_path: Absolute path to the folder with cleaned data files.
        db_params: Dictionary with PostgreSQL connection parameters
        schema_name: The name of the PostgreSQL schema to import data into.
                   (e.g., {'host': 'localhost', 'dbname': 'mydb', ...}).
        target_extensions: Set of file extensions to process. Defaults to {'.csv'}.

    Returns:
        A dictionary mapping original filenames to their import status messages.
    """
    if target_extensions is None:
        target_extensions = {'.csv', '.xlsx', '.xls', '.txt'}

    cleaned_path = Path(cleaned_data_folder_path).resolve()
    if not cleaned_path.is_dir():
        msg = f"Error: Cleaned data folder '{cleaned_data_folder_path}' does not exist or is not a directory."
        print(msg)
        return {"error_summary": msg}

    import_status: Dict[str, str] = {}
    conn = None
    try:
        print(f"\n--- Connecting to PostgreSQL: dbname='{db_params.get('dbname')}', host='{db_params.get('host')}' ---")
        conn = psycopg2.connect(**db_params)
        # conn.autocommit = False # Handled by explicit commit/rollback
        print("Successfully connected to PostgreSQL.")

        
        with conn.cursor() as cur:
            # Create schema if it doesn't exist
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(sql.Identifier(schema_name)))
            conn.commit()
            print(f"    Ensured schema '{schema_name}' exists.")

        all_files_in_cleaned_folder = [f for f in cleaned_path.iterdir() if f.is_file()]
        files_to_import = []
        print(f"Checking {len(all_files_in_cleaned_folder)} files in '{cleaned_path}':")
        for f in all_files_in_cleaned_folder:
            if f.suffix.lower() in target_extensions:
                files_to_import.append(f)
            else:
                print(f"  - Skipping '{f.name}' due to unsupported extension '{f.suffix}'. Target extensions: {target_extensions}")

        if not files_to_import:
            msg = f"No files with extensions {target_extensions} found in '{cleaned_path}'."
            print(msg)
            return {"summary_no_target_files": msg} # Changed key for clarity
            
        print(f"Found {len(files_to_import)} file(s) to process for import.")

        for file_path in tqdm(files_to_import, desc=f"Importing to PostgreSQL (schema: {schema_name})", unit="file", ncols=100):
            original_filename = file_path.name
            table_name = sanitize_identifier(original_filename) # Sanitize once for consistent table naming
            print(f"\n  Processing file: '{original_filename}' for table '{schema_name}.{table_name}'")
            
            if file_path.suffix.lower() == '.csv':
                with conn.cursor() as cur:
                    table_name = sanitize_identifier(original_filename)
                    print(f"\n  Processing file: '{original_filename}' for table '{schema_name}.{table_name}'")

                    try:
                        with open(file_path, 'r', encoding='utf-8', newline='') as f_csv_header: # Added newline=''
                            reader = csv_module.reader(f_csv_header)
                            header = next(reader)
                            if not header: raise ValueError("CSV file has an empty or missing header.")
                        
                        sanitized_cols_temp = [sanitize_identifier(col, is_column=True) for col in header]
                        # Sanitize and make CSV header columns unique
                        # Ensure unique column names after sanitization
                        final_columns = []
                        seen_cols = set()
                        for i, col_name in enumerate(sanitized_cols_temp):
                            unique_col_name = col_name
                            counter = 1
                            while unique_col_name in seen_cols:
                                unique_col_name = f"{col_name}_{counter}"
                                counter += 1
                            final_columns.append(unique_col_name)
                            seen_cols.add(unique_col_name)
                        
                        # --- Database Interaction ---
                        # 1. Create table if it doesn't exist
                        column_definitions_str = ", ".join([f"{sql.Identifier(col).string} TEXT" for col in final_columns])
                        
                        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                            sql.Identifier(schema_name),
                            sql.Identifier(table_name),
                            sql.SQL(column_definitions_str) # column_definitions_str is already safe
                        )
                        cur.execute(create_table_query)
                        conn.commit() # Commit the CREATE TABLE IF NOT EXISTS
                        print(f"    Ensured table '{schema_name}.{table_name}' exists.")

                        # 2. Get actual columns from the database table
                        db_columns = _get_table_columns(cur, schema_name, table_name)
                        
                        if db_columns is None:
                             error_msg = f"Skipping import for '{original_filename}': Could not retrieve existing table columns for '{schema_name}.{table_name}'."
                             print(f"    {error_msg}")
                             import_status[original_filename] = error_msg
                             continue # Skip to next file

                        # 3. Compare CSV header with database columns
                        if final_columns != db_columns:
                            error_msg = f"Skipping import for '{original_filename}': Schema mismatch with table '{schema_name}.{table_name}'.\n"
                            error_msg += f"      CSV Header (sanitized): {final_columns}\n"
                            error_msg += f"      DB Columns:             {db_columns}"
                            print(f"    [!] {error_msg}")
                            import_status[original_filename] = error_msg
                            continue # Skip to next file

                        # 4. If schemas match, proceed with COPY (append)
                        print(f"    Schema matches for '{schema_name}.{table_name}'. Proceeding with data import (append).")
                        # Use COPY FROM STDIN with HEADER
                        with open(file_path, 'r', encoding='utf-8', newline='') as f_copy: # Added newline=''
                            cur.copy_expert(sql.SQL("COPY {}.{} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"';").format(sql.Identifier(schema_name), sql.Identifier(table_name)), f_copy)
                        conn.commit()
                        import_status[original_filename] = f"Successfully imported to table '{schema_name}.{table_name}'."
                        print(f"    Successfully imported data from '{original_filename}'.")

                    except psycopg2.Error as db_copy_err: # More specific catch for DB errors during copy
                        conn.rollback()
                        err_details = f"DB Error during COPY. Code: {db_copy_err.pgcode}. Message: {db_copy_err.pgerror}"
                        if db_copy_err.diag:
                            diag = db_copy_err.diag
                            err_details += f"\n      Detail: {diag.message_detail}"
                            err_details += f"\n      Hint: {diag.message_hint}"
                            err_details += f"\n      Context: {diag.context}"
                            # source_line might not always be available or directly point to CSV line for STDIN
                            # but internal_query or source_position might give clues.
                            if diag.source_line: err_details += f"\n      Source Line: {diag.source_line}"
                            if diag.internal_query: err_details += f"\n      Internal Query: {diag.internal_query[:200]}..." # Truncate if long
                        error_msg = f"Error importing '{original_filename}': {err_details}"
                        print(f"    {error_msg}")
                        # Note: If the error is a schema mismatch detected by COPY itself (less likely now with explicit check),
                        # the error message will still be captured here.
                        import_status[original_filename] = error_msg
                    except Exception as e: # Catch other errors like file I/O, header issues, etc.
                        conn.rollback()
                        error_msg = f"Non-DB error importing '{original_filename}': {e}"
                        print(f"    {error_msg}")
                        import_status[original_filename] = error_msg
            elif file_path.suffix.lower() in ['.xls', '.xlsx']:
                print(f"    Attempting to process Excel file: {original_filename}")
                with conn.cursor() as cur:
                    try:
                        # Read Excel file - by default reads the first sheet
                        # For .xls, xlrd engine is often used. For .xlsx, openpyxl.
                        engine = 'xlrd' if file_path.suffix.lower() == '.xls' else 'openpyxl'
                        df = pd.read_excel(file_path, engine=engine, sheet_name=0) # Read first sheet

                        if df.empty:
                            msg = f"Excel file '{original_filename}' is empty or first sheet has no data."
                            print(f"    [!] {msg}")
                            import_status[original_filename] = msg
                            continue

                        # Sanitize column names from DataFrame
                        sanitized_df_cols_temp = [sanitize_identifier(str(col), is_column=True) for col in df.columns]
                        final_df_columns = []
                        seen_df_cols = set()
                        for col_name in sanitized_df_cols_temp:
                            unique_col_name = col_name
                            counter = 1
                            while unique_col_name in seen_df_cols:
                                unique_col_name = f"{col_name}_{counter}"
                                counter += 1
                            final_df_columns.append(unique_col_name)
                            seen_df_cols.add(unique_col_name)
                        df.columns = final_df_columns # Assign sanitized and unique columns back to DataFrame

                        # Create table if it doesn't exist
                        column_definitions_str = ", ".join([f"{sql.Identifier(col).string} TEXT" for col in final_df_columns])
                        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                            sql.Identifier(schema_name), sql.Identifier(table_name), sql.SQL(column_definitions_str)
                        )
                        cur.execute(create_table_query)
                        conn.commit()
                        print(f"    Ensured table '{schema_name}.{table_name}' exists for Excel import.")

                        # Get actual columns from the database table
                        db_columns = _get_table_columns(cur, schema_name, table_name)
                        if db_columns is None:
                            error_msg = f"Skipping Excel import for '{original_filename}': Could not retrieve table columns for '{schema_name}.{table_name}'."
                            print(f"    {error_msg}")
                            import_status[original_filename] = error_msg
                            continue

                        if final_df_columns != db_columns:
                            error_msg = f"Skipping Excel import for '{original_filename}': Schema mismatch with table '{schema_name}.{table_name}'.\n"
                            error_msg += f"      Excel Header (sanitized): {final_df_columns}\n"
                            error_msg += f"      DB Columns:               {db_columns}"
                            print(f"    [!] {error_msg}")
                            import_status[original_filename] = error_msg
                            continue
                        
                        # Convert DataFrame to CSV in memory
                        csv_buffer = io.StringIO()
                        df.to_csv(csv_buffer, index=False, header=True, quoting=csv_module.QUOTE_MINIMAL)
                        csv_buffer.seek(0) # Rewind buffer to the beginning

                        # Use COPY FROM STDIN
                        cur.copy_expert(sql.SQL("COPY {}.{} FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"';").format(sql.Identifier(schema_name), sql.Identifier(table_name)), csv_buffer)
                        conn.commit()
                        import_status[original_filename] = f"Successfully imported Excel data to table '{schema_name}.{table_name}'."
                        print(f"    Successfully imported data from Excel file '{original_filename}'.")
                    except Exception as e:
                        conn.rollback()
                        error_msg = f"Error importing Excel file '{original_filename}': {e}"
                        print(f"    {error_msg}")
                        import_status[original_filename] = error_msg

    except psycopg2.Error as db_err:
        error_msg = f"Database connection or general DB error: {db_err}"
        print(error_msg)
        if conn: conn.rollback()
        import_status["database_error_summary"] = error_msg
    except Exception as e:
        error_msg = f"An unexpected error occurred during DB import setup: {e}"
        print(error_msg)
        if conn: conn.rollback()
        import_status["unexpected_error_summary"] = error_msg
    finally:
        if conn:
            conn.close()
            print("\nPostgreSQL connection closed.")
            
    return import_status