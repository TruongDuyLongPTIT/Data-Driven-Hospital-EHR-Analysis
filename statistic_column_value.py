import pandas as pd
from ydata_profiling import ProfileReport

filename = "chartevents"

df = pd.read_csv(f'{filename}.csv.gz', nrows=150000)
profile = ProfileReport(df, title="Profiling Report", explorative=True)

# Xuất ra file HTML
profile.to_file(f"{filename}.html")

print(f"Report Profile đã được tạo: {filename}.html")
