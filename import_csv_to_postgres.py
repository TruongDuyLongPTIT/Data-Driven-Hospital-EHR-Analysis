import pandas as pd
import os
from pathlib import Path
from collections import defaultdict
import numpy as np

def detect_csv_relationships(folder_path, sample_size=1000):
    """
    Tự động detect relationships giữa các CSV files
    
    Args:
        folder_path: Đường dẫn folder chứa CSV files
        sample_size: Số rows để sample (tăng tốc độ)
    
    Returns:
        Dictionary chứa relationships detected
    """
    
    # Đọc tất cả CSV files (bao gồm .csv.gz)
    csv_files = list(Path(folder_path).glob("*.csv")) + list(Path(folder_path).glob("*.csv.gz"))
    tables = {}
    
    print(f"Đã tìm thấy {len(csv_files)} CSV files")
    
    # Load data từ các CSV (hỗ trợ compressed files)
    for file in csv_files:
        try:
            # Đọc sample data để tăng tốc độ (pandas tự động detect compression)
            df = pd.read_csv(file, nrows=sample_size, compression='infer')
            
            # Xử lý tên file cho .csv.gz
            table_name = file.name
            if table_name.endswith('.csv.gz'):
                table_name = table_name[:-7]  # Remove .csv.gz
            elif table_name.endswith('.csv'):
                table_name = table_name[:-4]  # Remove .csv
                
            tables[table_name] = df
            print(f"Loaded {table_name}: {df.shape}")
        except Exception as e:
            print(f"Error loading {file}: {e}")
    
    # Detect potential relationships
    relationships = []
    
    for table1_name, df1 in tables.items():
        for table2_name, df2 in tables.items():
            if table1_name >= table2_name:  # Tránh duplicate comparison
                continue
                
            # Check từng cột của table1 với table2
            for col1 in df1.columns:
                for col2 in df2.columns:
                    relationship = analyze_column_relationship(
                        df1, col1, table1_name,
                        df2, col2, table2_name
                    )
                    
                    if relationship:
                        relationships.append(relationship)
    
    return relationships

def analyze_column_relationship(df1, col1, table1_name, df2, col2, table2_name):
    """
    Phân tích mối quan hệ giữa 2 columns
    """
    
    # Skip nếu data types không compatible
    if not are_types_compatible(df1[col1].dtype, df2[col2].dtype):
        return None
    
    # Lấy unique values (remove nulls)
    values1 = set(df1[col1].dropna().astype(str))
    values2 = set(df2[col2].dropna().astype(str))
    
    if len(values1) == 0 or len(values2) == 0:
        return None
    
    # Calculate overlap
    intersection = values1.intersection(values2)
    overlap_ratio1 = len(intersection) / len(values1)
    overlap_ratio2 = len(intersection) / len(values2)
    
    # Thresholds để detect relationships
    HIGH_OVERLAP = 0.7
    MEDIUM_OVERLAP = 0.3
    
    # Detect relationship type
    relationship_type = None
    confidence = 0
    
    if overlap_ratio1 >= HIGH_OVERLAP and overlap_ratio2 >= HIGH_OVERLAP:
        relationship_type = "Strong Match (Many-to-Many)"
        confidence = min(overlap_ratio1, overlap_ratio2)
        
    elif overlap_ratio1 >= HIGH_OVERLAP and overlap_ratio2 < 0.5:
        relationship_type = f"Foreign Key ({table1_name} → {table2_name})"
        confidence = overlap_ratio1
        
    elif overlap_ratio2 >= HIGH_OVERLAP and overlap_ratio1 < 0.5:
        relationship_type = f"Foreign Key ({table2_name} → {table1_name})"
        confidence = overlap_ratio2
        
    elif overlap_ratio1 >= MEDIUM_OVERLAP or overlap_ratio2 >= MEDIUM_OVERLAP:
        relationship_type = "Partial Match"
        confidence = max(overlap_ratio1, overlap_ratio2)
    
    # Only return significant relationships
    if relationship_type and confidence >= MEDIUM_OVERLAP:
        return {
            'table1': table1_name,
            'column1': col1,
            'table2': table2_name,
            'column2': col2,
            'relationship_type': relationship_type,
            'confidence': round(confidence, 3),
            'overlap_count': len(intersection),
            'values1_count': len(values1),
            'values2_count': len(values2)
        }
    
    return None

def are_types_compatible(dtype1, dtype2):
    """
    Kiểm tra data types có compatible không
    """
    numeric_types = ['int64', 'int32', 'float64', 'float32']
    string_types = ['object', 'string']
    
    type1_str = str(dtype1)
    type2_str = str(dtype2)
    
    # Both numeric
    if type1_str in numeric_types and type2_str in numeric_types:
        return True
    
    # Both string-like
    if type1_str in string_types and type2_str in string_types:
        return True
    
    # Mixed but could be converted
    return True  # Liberal matching

def print_relationships(relationships):
    """
    In kết quả relationships dễ đọc
    """
    if not relationships:
        print("Không tìm thấy relationship nào!")
        return
    
    print(f"\n🔍 Tìm thấy {len(relationships)} potential relationships:")
    print("=" * 80)
    
    # Sort by confidence
    relationships.sort(key=lambda x: x['confidence'], reverse=True)
    
    for rel in relationships:
        print(f"""
📊 {rel['relationship_type']}
   {rel['table1']}.{rel['column1']} ↔ {rel['table2']}.{rel['column2']}
   Confidence: {rel['confidence']:.1%}
   Overlap: {rel['overlap_count']} values
   Details: {rel['values1_count']} vs {rel['values2_count']} unique values
        """)

def generate_erd_suggestion(relationships):
    """
    Tạo suggestions cho ERD
    """
    print("\n🎯 ERD Suggestions:")
    print("-" * 50)
    
    foreign_keys = [r for r in relationships if "Foreign Key" in r['relationship_type']]
    strong_matches = [r for r in relationships if "Strong Match" in r['relationship_type']]
    
    if foreign_keys:
        print("🔑 Potential Foreign Keys:")
        for fk in foreign_keys:
            print(f"   {fk['table1']}.{fk['column1']} → {fk['table2']}.{fk['column2']}")
    
    if strong_matches:
        print("\n🔗 Strong Relationships:")
        for sm in strong_matches:
            print(f"   {sm['table1']}.{sm['column1']} ↔ {sm['table2']}.{sm['column2']}")

# Main execution
if __name__ == "__main__":
    # Thay đổi path này thành folder chứa CSV files của bạn
    folder_path = r"C:\DataUser\MIMIC Dataset\mimic-iv-3.1\hosp"  # Current directory
    
    print("🚀 Starting CSV Relationship Detection...")
    
    # Detect relationships
    relationships = detect_csv_relationships(folder_path, sample_size=1000)
    
    # Print results
    print_relationships(relationships)
    generate_erd_suggestion(relationships)
    
    print("\n✅ Analysis completed!")