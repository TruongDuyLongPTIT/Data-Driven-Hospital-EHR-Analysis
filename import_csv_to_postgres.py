import psycopg2
import gzip


MIMIC_TABLES = [
    "admissions",
    "caregiver",
    "chartevents",
    "d_hcpcs",
    "d_icd_diagnoses",
    "d_icd_procedures",
    "d_items",
    "d_labitems",
    "datetimeevents",
    "diagnoses_icd",
    "drgcodes",
    "emar",
    "emar_detail",
    "hcpcsevents",
    "icustays",
    "ingredientevents",
    "inputevents",
    "labevents",
    "microbiologyevents",
    "omr",
    "outputevents",
    "patients",
    "pharmacy",
    "poe",
    "poe_detail",
    "prescriptions",
    "procedureevents",
    "procedures_icd",
    "services",
    "transfers"
]

conn = psycopg2.connect("dbname=postgres user=admin password=admin host=localhost")
cur = conn.cursor()

for table_name in MIMIC_TABLES:
    with gzip.open(f"C:/DataUser/MIMIC Dataset/mimic-iv-3.1/hosp/{table_name}.csv.gz", "rt", encoding="utf-8") as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN CSV HEADER", f)

conn.commit()
cur.close()
conn.close()
