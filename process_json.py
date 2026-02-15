import json
import boto3
import os
from datetime import datetime

ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
SECRET_KEY = os.environ["R2_SECRET_KEY"]

BUCKET_NAME = "tables"
RAW_KEY = "raw/bigdata.json"

SETS = 10
TABLES_PER_SET = 10
TOTAL_NEEDED = SETS * TABLES_PER_SET

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

day_of_year = datetime.utcnow().timetuple().tm_yday
start_index = (day_of_year * TOTAL_NEEDED) % TOTAL_TABLES
end_index = start_index + TOTAL_NEEDED

selected_tables = tables[start_index:end_index]

# Eğer sonu aşarsa başa sar
if len(selected_tables) < TOTAL_NEEDED:
    remaining = TOTAL_NEEDED - len(selected_tables)
    selected_tables += tables[0:remaining]

# 10 ayrı dosya üret
for i in range(SETS):
    start = i * TABLES_PER_SET
    end = start + TABLES_PER_SET
    subset = selected_tables[start:end]

    filename = f"set{i+1}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(subset, f, ensure_ascii=False)

    s3.upload_file(filename, BUCKET_NAME, f"public/{filename}")

print("10 set başarıyla yüklendi.")
