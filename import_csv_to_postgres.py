import psycopg2
import gzip
import os
import time
from tqdm import tqdm


MIMIC_TABLES = [
    # "admissions",
    # "caregiver", 
    # "chartevents",
    "d_hcpcs",
    # "d_icd_diagnoses",
    # "d_icd_procedures",
    # "d_items",
    # "d_labitems",
    # "datetimeevents",
    # "diagnoses_icd",
    # "drgcodes",
    # "emar",
    # "emar_detail", 
    # "hcpcsevents",
    "icustays"
    # "ingredientevents",
    # "inputevents",
    # "labevents",
    # "microbiologyevents",
    # "omr",
    # "outputevents",
    # "patients",
    # "pharmacy",
    # "poe",
    # "poe_detail",
    # "prescriptions",
    # "procedureevents",
    # "procedures_icd",
    # "services",
    # "transfers"
]


def check_database_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect("dbname=postgres user=admin password=admin host=localhost port=5432")
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"✅ Database connected successfully: {version[0]}")
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False


def check_file_exists(file_path):
    """Check if file exists and readable"""
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return False
    
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            # Try to read first line
            first_line = f.readline()
            if first_line:
                print(f"✅ File readable: {file_path} (first line: {first_line[:50]}...)")
                return True
            else:
                print(f"❌ File empty: {file_path}")
                return False
    except Exception as e:
        print(f"❌ Cannot read file {file_path}: {e}")
        return False


def create_table_if_not_exists(cur, table_name, file_path):
    """Create table based on CSV header if not exists"""
    try:
        # Check if table exists
        cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = %s
        );
        """, (table_name,))
        
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            print(f"📋 Table '{table_name}' already exists")
            return True
        
        print(f"📋 Table '{table_name}' not found, creating...")
        
        # Read CSV header to create table schema
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            header = f.readline().strip().split(',')
            
        # Create table with TEXT columns (safe default)
        columns = [f'"{col.strip()}" TEXT' for col in header]
        create_sql = f'CREATE TABLE "{table_name}" ({", ".join(columns)});'
        
        print(f"🔨 Creating table: {create_sql[:100]}...")
        cur.execute(create_sql)
        print(f"✅ Table '{table_name}' created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create table '{table_name}': {e}")
        return False


def import_csv_to_table(cur, table_name, file_path):
    """Import CSV to PostgreSQL table with progress tracking"""
    try:
        print(f"\n🔄 Importing {table_name}...")
        start_time = time.time()
        
        # Get file size for progress tracking
        file_size = os.path.getsize(file_path)
        print(f"📊 File size: {file_size / (1024*1024):.1f} MB")
        
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            # Use COPY command with error handling
            try:
                cur.copy_expert(f'COPY "{table_name}" FROM STDIN CSV HEADER', f)
                
                # Get imported row count
                cur.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                row_count = cur.fetchone()[0]
                
                elapsed_time = time.time() - start_time
                print(f"✅ {table_name}: {row_count:,} rows imported in {elapsed_time:.1f}s")
                return True
                
            except psycopg2.Error as e:
                print(f"❌ COPY command failed for {table_name}: {e}")
                print(f"   Error code: {e.pgcode}")
                print(f"   Error details: {e.pgerror}")
                return False
                
    except Exception as e:
        print(f"❌ Failed to import {table_name}: {e}")
        return False


def main():
    print("🚀 MIMIC-IV CSV Import Script")
    print("=" * 50)
    
    # Check database connection first
    if not check_database_connection():
        print("Please check your PostgreSQL service and credentials")
        return
    
    # Check if all files exist
    base_path = "C:/DataUser/MIMIC Dataset/mimic-iv-3.1/hosp"
    missing_files = []
    
    print(f"\n📁 Checking files in: {base_path}")
    for table_name in MIMIC_TABLES:
        file_path = f"{base_path}/{table_name}.csv.gz"
        if not check_file_exists(file_path):
            missing_files.append(table_name)
    
    if missing_files:
        print(f"\n❌ Missing files for tables: {missing_files}")
        print("Please check file paths and names")
        return
    
    # Connect to database
    try:
        conn = psycopg2.connect(
            "dbname=postgres user=admin password=admin host=localhost port=5432",
            options="-c statement_timeout=0"  # No timeout for large imports
        )
        conn.set_session(autocommit=False)  # Use transactions
        cur = conn.cursor()
        
        print(f"\n🔄 Starting import of {len(MIMIC_TABLES)} tables...")
        
        successful_imports = 0
        failed_imports = []
        
        for i, table_name in enumerate(MIMIC_TABLES, 1):
            print(f"\n[{i}/{len(MIMIC_TABLES)}] Processing {table_name}...")
            
            file_path = f"{base_path}/{table_name}.csv.gz"
            
            try:
                # Create table if not exists
                if not create_table_if_not_exists(cur, table_name, file_path):
                    failed_imports.append(table_name)
                    continue
                
                # Import data
                if import_csv_to_table(cur, table_name, file_path):
                    successful_imports += 1
                    conn.commit()  # Commit after each successful table
                    print(f"💾 {table_name} committed to database")
                else:
                    failed_imports.append(table_name)
                    conn.rollback()  # Rollback failed import
                    
            except Exception as e:
                print(f"❌ Unexpected error processing {table_name}: {e}")
                failed_imports.append(table_name)
                conn.rollback()
        
        # Final summary
        print("\n" + "=" * 50)
        print("📊 IMPORT SUMMARY")
        print("=" * 50)
        print(f"✅ Successful imports: {successful_imports}")
        print(f"❌ Failed imports: {len(failed_imports)}")
        
        if failed_imports:
            print(f"Failed tables: {', '.join(failed_imports)}")
        
        # Show table sizes
        print(f"\n📋 Table sizes:")
        cur.execute("""
        SELECT table_name, 
               pg_size_pretty(pg_total_relation_size(quote_ident(table_name))) as size
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = ANY(%s)
        ORDER BY pg_total_relation_size(quote_ident(table_name)) DESC;
        """, (MIMIC_TABLES,))
        
        for row in cur.fetchall():
            print(f"   {row[0]}: {row[1]}")
            
    except Exception as e:
        print(f"❌ Database error: {e}")
        
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        print(f"\n🏁 Import process completed")


def quick_test():
    """Quick test with one small table"""
    print("🧪 Quick test with 'patients' table...")
    
    try:
        conn = psycopg2.connect("dbname=postgres user=admin password=admin host=localhost port=5432")
        cur = conn.cursor()
        
        table_name = "icustays"
        file_path = f"C:/DataUser/MIMIC Dataset/mimic-iv-3.1/hosp/{table_name}.csv.gz"
        
        if check_file_exists(file_path):
            create_table_if_not_exists(cur, table_name, file_path)
            import_csv_to_table(cur, table_name, file_path)
            conn.commit()
            print("✅ Quick test completed successfully!")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Quick test failed: {e}")


if __name__ == "__main__":
    print("Choose option:")
    print("1. Full import (all tables)")
    print("2. Quick test (patients table only)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "2":
        quick_test()
    else:
        main()