import os
import csv
import re
import psycopg2

# === CONFIGURATION ===
DATA_FOLDER = 'data'  # Folder containing the files
POSTGRES_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '74584',
    'host': 'localhost',
    'port': '5432'
}

# === HELPER FUNCTIONS FOR SANITIZATION ===
def sanitize_column_name(col, index):
    """
    Sanitizes a column header by stripping whitespace, replacing spaces/hyphens
    with underscores, removing non-word characters, and ensuring a non-empty value.
    If the column is empty, a default name "col_<index>" is assigned.
    Also prepends 'col_' if the column name starts with a digit.
    """
    new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
    new_col = re.sub(r'\W+', '', new_col)
    if not new_col:
        new_col = f"col_{index}"
    if new_col[0].isdigit():
        new_col = "col_" + new_col
    return new_col

def sanitize_table_name(file_basename):
    """
    Sanitizes a file basename to create a valid table name:
    - Lowercases, replaces spaces/hyphens with underscores,
    - Removes non-word characters,
    - Assigns a default if empty,
    - And prepends 't_' if the name starts with a digit.
    """
    table_name = file_basename.lower().replace(" ", "_").replace("-", "_")
    table_name = re.sub(r'\W+', '', table_name)
    if not table_name:
        table_name = "table"
    if table_name[0].isdigit():
        table_name = "t_" + table_name
    return table_name

# === CLEAN CSV FILE ===
def clean_csv(input_path, output_path):
    """
    Cleans the CSV by removing empty rows, stripping whitespace,
    and converting line breaks to spaces.
    """
    with open(input_path, 'r', encoding='cp1252', newline='') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            if any(cell.strip() for cell in row):  # Skip empty rows
                cleaned_row = [
                    cell.strip()
                        .replace('\r\n', ' ')
                        .replace('\n', ' ')
                        .replace('\r', ' ')
                    for cell in row
                ]
                writer.writerow(cleaned_row)
    print(f"[✓] Cleaned CSV saved as: {output_path}")

# === GENERATE SQL SCHEMA FROM HEADER ===
def generate_create_table_sql(header, table_name):
    """
    Generates a CREATE TABLE statement for a list of column headers.
    By default, columns are typed as TEXT for maximum compatibility.
    Utilizes sanitized column names.
    """
    cols = []
    for i, col in enumerate(header):
        safe_col = sanitize_column_name(col, i)
        cols.append(f'"{safe_col}" TEXT')
    return f'CREATE TABLE IF NOT EXISTS {table_name} (\n  {",\n  ".join(cols)}\n);'

# === IMPORT CLEANED CSV TO POSTGRES ===
def import_csv_to_postgres(clean_path, table_name, config):
    """
    Creates or verifies a table matching the CSV columns, then imports
    the CSV data using PostgreSQL's COPY command.
    """
    try:
        with open(clean_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # Capture header separately

            create_sql = generate_create_table_sql(header, table_name)

            conn = psycopg2.connect(**config)
            cur = conn.cursor()

            # Create the table if it doesn't exist
            cur.execute(create_sql)
            conn.commit()
            print(f"[✓] Table `{table_name}` created or verified.")

            # Rewind the file pointer for copy_expert
            f.seek(0)
            cur.copy_expert(
                f"COPY {table_name} FROM STDIN WITH CSV HEADER ENCODING 'UTF8'",
                f
            )
            conn.commit()
            print(f"[✓] Data imported successfully into `{table_name}`.")
    except Exception as e:
        print(f"[✗] Error importing {clean_path} into `{table_name}`: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

# === CREATE TABLE FOR STORING NON-CSV FILES ===
def create_raw_files_table(config):
    """
    Creates (if needed) the raw_files table, which stores arbitrary files
    as binary data. This includes xlsx, txt, shp, sbx, dbf, etc.
    """
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS raw_files (
            id SERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            file_extension TEXT,
            file_data BYTEA
        );
    """
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute(create_table_sql)
        conn.commit()
        print("[✓] Table `raw_files` created or verified.")
    except Exception as e:
        print(f"[✗] Error creating/verifying raw_files table: {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

# === IMPORT NON-CSV FILE AS BINARY DATA ===
def import_file_as_raw(file_path, config):
    """
    Inserts a single file (any extension except CSV) into the raw_files table
    as binary data (BYTEA).
    """
    filename = os.path.basename(file_path)
    extension = os.path.splitext(filename)[1]  # e.g. ".xlsx", ".txt", etc.

    # Read the file as binary
    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()

        conn = psycopg2.connect(**config)
        cur = conn.cursor()

        # Insert the file into raw_files
        insert_sql = """
            INSERT INTO raw_files (filename, file_extension, file_data)
            VALUES (%s, %s, %s);
        """
        cur.execute(insert_sql, (filename, extension, psycopg2.Binary(file_data)))
        conn.commit()
        print(f"[✓] File `{filename}` imported as binary data into `raw_files`.")

    except Exception as e:
        print(f"[✗] Error importing file `{filename}` as raw: {e}")

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()

# === MAIN WORKFLOW ===
def main():
    # Ensure the data folder exists
    if not os.path.isdir(DATA_FOLDER):
        print(f"[✗] Folder '{DATA_FOLDER}' not found.")
        return

    # 1. Create the table for raw files if it doesn't exist
    create_raw_files_table(POSTGRES_CONFIG)

    # 2. Process each file in the folder
    for file_name in os.listdir(DATA_FOLDER):
        # Full path to file
        file_path = os.path.join(DATA_FOLDER, file_name)
        if not os.path.isfile(file_path):
            # Skip directories or anything that isn't a file
            continue

        # Check the file extension
        ext = os.path.splitext(file_name)[1].lower()
        
        if ext == '.csv':
            # Process CSV with cleaning + specialized import
            cleaned_csv_path = os.path.join(
                DATA_FOLDER,
                f"{os.path.splitext(file_name)[0]}_cleaned.csv"
            )
            print(f"\n[→] Detected CSV file: {file_name}")
            clean_csv(file_path, cleaned_csv_path)
            
            # Use the sanitized base name (no extension) as the table name
            base_name = os.path.splitext(file_name)[0]
            table_name = sanitize_table_name(base_name)
            import_csv_to_postgres(cleaned_csv_path, table_name, POSTGRES_CONFIG)

        else:
            # Import all other file types as binary data
            print(f"\n[→] Detected non-CSV file: {file_name}")
            import_file_as_raw(file_path, POSTGRES_CONFIG)

if __name__ == '__main__':
    main()
