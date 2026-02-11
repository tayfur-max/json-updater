import json
import boto3
import os
from datetime import datetime

# R2 bilgileri
ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
SECRET_KEY = os.environ["R2_SECRET_KEY"]

BUCKET_NAME = "tables"
RAW_KEY = "raw/bigdata.json"
OUTPUT_KEY = "public/latest.json"

# R2 bağlantı
s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

# Büyük dosyayı indir
s3.download_file(BUCKET_NAME, RAW_KEY, "bigdata.json")

# JSON oku
with open("bigdata.json", "r", encoding="utf-8") as f:
    tables = json.load(f)

TOTAL_TABLES = len(tables)
TABLES_PER_DAY = 50

day_of_year = datetime.utcnow().timetuple().tm_yday
start_index = (day_of_year * TABLES_PER_DAY) % TOTAL_TABLES
end_index = start_index + TABLES_PER_DAY

selected_tables = tables[start_index:end_index]

if len(selected_tables) < TABLES_PER_DAY:
    remaining = TABLES_PER_DAY - len(selected_tables)
    selected_tables += tables[0:remaining]

with open("latest.json", "w", encoding="utf-8") as f:
    json.dump(selected_tables, f, ensure_ascii=False)

# Küçük dosyayı R2'ye upload et
s3.upload_file("latest.json", BUCKET_NAME, OUTPUT_KEY)

print("Upload başarılı.")
