import json
import boto3
import os
from datetime import datetime

ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
SECRET_KEY = os.environ["R2_SECRET_KEY"]

BUCKET_NAME = "tables"
RAW_KEY = "raw/bigdata.json"

TOTAL_FILES = 100  # 100 ayrı json üretilecek

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Büyük dosyayı indir
s3.download_file(BUCKET_NAME, RAW_KEY, "bigdata.json")

with open("bigdata.json", "r", encoding="utf-8") as f:
    tables = json.load(f)

TOTAL_TABLES = len(tables)

if TOTAL_TABLES == 0:
    raise Exception("JSON içinde tablo yok!")

# Gün bazlı başlangıç indexi
day_of_year = datetime.utcnow().timetuple().tm_yday
start_index = (day_of_year * TOTAL_FILES) % TOTAL_TABLES

selected_tables = []

for i in range(TOTAL_FILES):
    index = (start_index + i) % TOTAL_TABLES
    selected_tables.append(tables[index])

# 100 ayrı dosya üret (her biri 1 tablo)
for i, table in enumerate(selected_tables):
    filename = f"table_{i+1}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(table, f, ensure_ascii=False)

    s3.upload_file(filename, BUCKET_NAME, f"public/{filename}")

print("100 tablo başarıyla üretildi ve yüklendi.")
