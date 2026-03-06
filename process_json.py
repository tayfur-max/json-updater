import json
import boto3
import os
from datetime import datetime

ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
SECRET_KEY = os.environ["R2_SECRET_KEY"]

BUCKET_NAME = "tables"

TOTAL_FILES = 100

DATASETS = {
    "classic": "raw/bigdata.json",
    "ligue1": "raw/ligue1.json",
    "premier_ligue": "raw/premier_ligue.json",
    "super_ligue": "raw/super_ligue.json"
}

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

day_of_year = datetime.utcnow().timetuple().tm_yday

for mode, key in DATASETS.items():

    local_file = f"{mode}.json"

    print(f"{mode} indiriliyor...")

    s3.download_file(BUCKET_NAME, key, local_file)

    with open(local_file, "r", encoding="utf-8") as f:
        tables = json.load(f)

    TOTAL_TABLES = len(tables)

    if TOTAL_TABLES == 0:
        raise Exception(f"{mode} JSON içinde tablo yok!")

    start_index = (day_of_year * TOTAL_FILES) % TOTAL_TABLES

    for i in range(TOTAL_FILES):

        index = (start_index + i) % TOTAL_TABLES
        table = tables[index]

        filename = f"{mode}_table_{i+1}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(table, f, ensure_ascii=False)

        s3.upload_file(
            filename,
            BUCKET_NAME,
            f"public/{mode}/{filename}"
        )

    print(f"{mode} için 100 tablo üretildi.")

print("Tüm modlar tamamlandı.")
