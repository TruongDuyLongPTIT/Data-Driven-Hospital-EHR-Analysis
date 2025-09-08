#!/usr/bin/env python3
import polars as pl
import psycopg2
import sys
import os
import glob

# Config
CONN = "postgresql://admin:admin@localhost:5432/postgres"

def check_connection():
    try:
        conn = psycopg2.connect(CONN)
        conn.close()
        print("✅ Kết nối PostgreSQL thành công!")
        return True
    except Exception as e:
        print(f"❌ Không thể kết nối PostgreSQL: {e}")
        return False

def import_csv_sample(file_path, table_name, n_rows=100):
    """
    Chỉ đọc n_rows đầu tiên để import vào Postgres
    """
    print(f"\n📥 Importing sample from {file_path} → {table_name}")

    common_nulls = ["___", ".", "NA", "N/A", "NULL", "UNKNOWN", "?", ""]

    try:
        df = pl.read_csv(
            file_path,
            n_rows=n_rows,
            infer_schema_length=n_rows,      # đoán kiểu dựa trên sample
            null_values=common_nulls,
            schema_overrides={
                "icd_code": pl.Utf8,
                "dose_given": pl.Float64,
                "complete_flag": pl.Utf8,
            },
            ignore_errors=True               # bỏ qua giá trị không parse được
        )
    except Exception as e:
        print(f"❌ Lỗi khi đọc {file_path}: {e}")
        return

    try:
        df.write_database(
            table_name=table_name,
            connection=CONN,
            if_table_exists="replace"  # luôn replace để dễ chỉnh sửa
        )
        print(f"✅ Done: {table_name} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Lỗi khi ghi vào Postgres: {e}")

if __name__ == "__main__":

    folder = r"C:\DataUser\MIMIC Dataset\mimic-iv-3.1\hosp"

    if check_connection():
        files = glob.glob(os.path.join(folder, "*.csv.gz"))
        if not files:
            print("❌ Không tìm thấy file .csv.gz trong folder.")
            exit(1)

        for file_path in files:
            table_name = os.path.splitext(os.path.splitext(os.path.basename(file_path))[0])[0]
            import_csv_sample(file_path, table_name, n_rows=100)
