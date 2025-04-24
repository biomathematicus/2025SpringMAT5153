import os
import csv
import re
import zipfile
import shutil
import psycopg2
import pandas as pd
import argparse

# === CONFIGURATION ===
PWD = os.getenv("PostgreSQL_PWD")
DATA_FOLDER = 'data'
EXTRACTED_FOLDER = 'extracted_data'
CLEANED_FOLDER = 'cleaned_data'

# **NEW**: import into this schema instead of public
SCHEMA_NAME = 'crdc_import'

POSTGRES_CONFIG = {
    'dbname':   'postgres',
    'user':     'postgres',
    'password': PWD,
    'host':     'localhost',
    'port':     '5432'
}

# parse import mode
parser = argparse.ArgumentParser(description="Import CRDC files to Postgres")
parser.add_argument(
    '--mode',
    choices=['normal', 'overwrite_all', 'import_missing'],
    default='normal',
    help=(
        "normal = prompt each table (default), "
        "overwrite_all = drop & reimport everything, "
        "import_missing = only import new tables"
    )
)
args = parser.parse_args()
IMPORT_MODE = args.mode

# === SCHEMA SETUP ===
def ensure_schema(schema, config):
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
            print(f"[✓] Schema `{schema}` ready.\n")

# === SANITIZATION HELPERS ===
def sanitize_column_name(col, index):
    new_col = col.strip().lower().replace(" ", "_").replace("-", "_")
    new_col = re.sub(r'\W+', '', new_col)
    if not new_col:
        new_col = f"col_{index}"
    if new_col[0].isdigit():
        new_col = "col_" + new_col
    return new_col

def sanitize_table_name(file_basename):
    table_name = file_basename.lower().replace(" ", "_").replace("-", "_")
    table_name = re.sub(r'\W+', '', table_name)
    if not table_name:
        table_name = "table"
    if table_name[0].isdigit():
        table_name = "t_" + table_name
    return table_name

def table_exists(table_name, config):
    """Check existence in SCHEMA_NAME instead of public."""
    sql = """
      SELECT EXISTS(
        SELECT 1
          FROM pg_catalog.pg_tables
         WHERE schemaname = %s
           AND tablename = %s
      );
    """
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute(sql, (SCHEMA_NAME, table_name))
        return cur.fetchone()[0]
    except Exception as e:
        print(f"[✗] Error checking `{table_name}`: {e}")
        return False
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def handle_overwrite(table_name, config):
    exists = table_exists(table_name, config)
    if not exists:
        return True

    qualified = f"{SCHEMA_NAME}.{table_name}"
    if IMPORT_MODE == 'overwrite_all':
        try:
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE;")
            conn.commit()
            print(f"[✓] (overwrite_all) Dropped `{qualified}`.")
        except Exception as e:
            print(f"[✗] Error dropping `{qualified}`: {e}")
            return False
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()
        return True

    if IMPORT_MODE == 'import_missing':
        print(f"[!] (import_missing) Skipping existing `{qualified}`.")
        return False

    # normal mode: prompt
    ans = input(f"Table `{qualified}` exists. Overwrite? (y/n): ").strip().lower()
    if ans == 'y':
        try:
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {qualified} CASCADE;")
            conn.commit()
            print(f"[✓] Dropped `{qualified}`.")
        except Exception as e:
            print(f"[✗] Error dropping `{qualified}`: {e}")
            return False
        finally:
            if 'cur' in locals(): cur.close()
            if 'conn' in locals(): conn.close()
        return True
    else:
        print(f"[!] Skipping `{qualified}`.")
        return False

# === ZIP EXTRACTION ===
def extract_zip_files(source_folder, destination_folder):
    os.makedirs(destination_folder, exist_ok=True)
    for root, _, files in os.walk(source_folder):
        for fname in files:
            if fname.lower().endswith('.zip'):
                zip_path = os.path.join(root, fname)
                try:
                    with zipfile.ZipFile(zip_path, 'r') as z:
                        z.extractall(destination_folder)
                    rel = os.path.relpath(zip_path, source_folder)
                    print(f"[✓] Extracted ZIP: {rel}")
                except Exception as e:
                    print(f"[✗] Error extracting `{zip_path}`: {e}")
    for fname in os.listdir(source_folder):
        path = os.path.join(source_folder, fname)
        if os.path.isfile(path) and not fname.lower().endswith('.zip'):
            try:
                shutil.copy(path, destination_folder)
                print(f"[✓] Copied file: {fname}")
            except Exception as e:
                print(f"[✗] Error copying `{fname}`: {e}")
    print()

# === CSV CLEANING ===
def clean_csv(input_path, output_path):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(input_path, 'r', encoding='cp1252', newline='') as infile, \
         open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        for row in reader:
            if any(cell.strip() for cell in row):
                cleaned = [cell.strip()
                           .replace('\r\n',' ')
                           .replace('\n',' ')
                           .replace('\r',' ')
                           for cell in row]
                writer.writerow(cleaned)
    print(f"[✓] Cleaned CSV: {os.path.relpath(output_path, CLEANED_FOLDER)}")

# === SQL SCHEMA GENERATION ===
def generate_create_table_sql(header, table_name):
    cols = []
    for i, col in enumerate(header):
        safe = sanitize_column_name(col, i)
        cols.append(f'"{safe}" TEXT')
    sep = ",\n  "
    qualified = f"{SCHEMA_NAME}.{table_name}"
    return f"CREATE TABLE {qualified} (\n  " + sep.join(cols) + "\n);"

# === IMPORT CSV ===
def import_csv_to_postgres(clean_path, table_name, config):
    if not handle_overwrite(table_name, config):
        return
    qualified = f"{SCHEMA_NAME}.{table_name}"
    try:
        with open(clean_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            create_sql = generate_create_table_sql(header, table_name)
            conn = psycopg2.connect(**config)
            cur = conn.cursor()
            cur.execute(create_sql)
            conn.commit()
            print(f"[✓] Created `{qualified}`.")
            f.seek(0)
            cur.copy_expert(
                f"COPY {qualified} FROM STDIN WITH CSV HEADER ENCODING 'UTF8'",
                f
            )
            conn.commit()
            print(f"[✓] Imported `{qualified}`.\n")
    except Exception as e:
        print(f"[✗] Error importing `{qualified}`: {e}\n")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# === IMPORT EXCEL ===
def import_excel_to_postgres(file_path, table_name, config):
    if not handle_overwrite(table_name, config):
        return
    try:
        df = pd.read_excel(file_path)
        temp_csv = os.path.join(CLEANED_FOLDER, f"{table_name}_converted.csv")
        os.makedirs(os.path.dirname(temp_csv), exist_ok=True)
        df.to_csv(temp_csv, index=False)
        print(f"[✓] Converted Excel to CSV: {os.path.basename(temp_csv)}")
        import_csv_to_postgres(temp_csv, table_name, config)
        os.remove(temp_csv)
    except Exception as e:
        print(f"[✗] Error importing Excel `{file_path}`: {e}\n")

# === RAW FILES TABLE ===
def create_raw_files_table(config):
    qualified = f"{SCHEMA_NAME}.raw_files"
    sql = f"""
    CREATE TABLE IF NOT EXISTS {qualified} (
      id SERIAL PRIMARY KEY,
      filename TEXT NOT NULL,
      file_extension TEXT,
      file_data BYTEA
    );
    """
    try:
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        print(f"[✓] `{qualified}` table ready.\n")
    except Exception as e:
        print(f"[✗] Error creating `{qualified}`: {e}\n")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

def import_file_as_raw(file_path, config):
    qualified = f"{SCHEMA_NAME}.raw_files"
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1]
    try:
        with open(file_path,'rb') as f:
            data = f.read()
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO {qualified} (filename,file_extension,file_data) VALUES (%s,%s,%s)",
            (filename, ext, psycopg2.Binary(data))
        )
        conn.commit()
        print(f"[✓] Raw imported: {filename}\n")
    except Exception as e:
        print(f"[✗] Error raw `{filename}`: {e}\n")
    finally:
        if 'cur' in locals(): cur.close()
        if 'conn' in locals(): conn.close()

# === MAIN WORKFLOW ===
def main():
    # 0) ensure target schema exists
    ensure_schema(SCHEMA_NAME, POSTGRES_CONFIG)

    # 1) ensure folders
    for d in (DATA_FOLDER, EXTRACTED_FOLDER, CLEANED_FOLDER):
        os.makedirs(d, exist_ok=True)

    # 2) extract zips
    extract_zip_files(DATA_FOLDER, EXTRACTED_FOLDER)
    # 3) create raw_files in new schema
    create_raw_files_table(POSTGRES_CONFIG)

    # 4) import each file
    for root, _, files in os.walk(EXTRACTED_FOLDER):
        for fname in files:
            path = os.path.join(root, fname)
            rel  = os.path.relpath(path, EXTRACTED_FOLDER)
            ext  = os.path.splitext(fname)[1].lower()
            tbl  = sanitize_table_name(os.path.splitext(fname)[0])

            print("\n" + "="*60)
            print(f"Processing: {rel}")
            print("="*60)

            if ext == '.csv':
                cleaned = os.path.join(CLEANED_FOLDER, rel)
                clean_csv(path, cleaned)
                import_csv_to_postgres(cleaned, tbl, POSTGRES_CONFIG)

            elif ext in ('.xlsx', '.xls'):
                import_excel_to_postgres(path, tbl, POSTGRES_CONFIG)

            else:
                import_file_as_raw(path, POSTGRES_CONFIG)

if __name__ == '__main__':
    main()
