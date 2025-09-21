import psycopg2
import gzip

conn = psycopg2.connect("dbname=mydb user=admin password=admin host=localhost")
cur = conn.cursor()

with gzip.open(r"C:\DataUser\MIMIC Dataset\mimic-iv-3.1\hosp\chartevents.csv.gz", "rt", encoding="utf-8") as f:
    cur.copy_expert("COPY chartevents FROM STDIN CSV HEADER", f)

conn.commit()
cur.close()
conn.close()
