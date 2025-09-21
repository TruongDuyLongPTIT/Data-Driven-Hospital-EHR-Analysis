import bar_chart_race as bcr
import pandas as pd
import numpy as np

# Danh sách bệnh (thêm vài bệnh "quen thuộc")
core_diseases = ["Cúm", "Viêm phổi", "Covid-19", "Tiểu đường", "Tim mạch"]
extra_diseases = [f"Bệnh {i}" for i in range(6, 51)]  # tạo thêm 45 bệnh
diseases = core_diseases + extra_diseases

# Sinh dữ liệu cho từng tuổi
ages = list(range(0, 101))  # 0,1,2,...100
data = {}

np.random.seed(42)  # fix random cho reproducibility

for age in ages:
    age_data = {}
    for d in diseases:
        if d in core_diseases:
            cases = np.random.randint(100, 1000)  # bệnh "quen thuộc" nhiều ca hơn
        else:
            cases = np.random.randint(10, 300)
        age_data[d] = cases
    # chọn top 10 bệnh phổ biến nhất cho tuổi này
    top10 = dict(sorted(age_data.items(), key=lambda x: x[1], reverse=True)[:10])
    data[age] = top10

# Chuyển thành DataFrame dạng wide (tuổi = index, bệnh = columns)
df = pd.DataFrame(data).T.fillna(0).astype(int)

# Xuất video bar chart race
bcr.bar_chart_race(
    df=df,
    filename='barchart_race.mp4',
    title='Top 10 bệnh phổ biến theo độ tuổi (0-100)',
    n_bars=10,
    steps_per_period=10,
    period_length=500  # ms cho mỗi bước
)
