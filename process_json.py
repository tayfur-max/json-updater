import json
import boto3
import os
from datetime import datetime

ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
SECRET_KEY = os.environ["R2_SECRET_KEY"]

BUCKET_NAME = "tables"

# İşlenecek 4 farklı json
JSON_SOURCES = [
    {
        "name": "klasik",
        "raw_key": "raw/klasik.json",
        "output_prefix": "public/klasik_",
        "file_count": 10
    },
    {
        "name": "superlig",
        "raw_key": "raw/superlig.json",
        "output_prefix": "public/superlig_",
        "file_count": 10
    },
    {
        "name": "ligue1",
        "raw_key": "raw/ligue1.json",
        "output_prefix": "public/ligue1_",
        "file_count": 10
    },
    {
        "name": "premierlig",
        "raw_key": "raw/premierlig.json",
        "output_prefix": "public/premierlig_",
        "file_count": 10
    }
]

s3 = boto3.client(
    "s3",
    endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)

day_of_year = datetime.utcnow().timetuple().tm_yday

for source in JSON_SOURCES:
    print(f"İşleniyor: {source['name']}")

    # Raw dosyayı indir
    local_raw_file = f"{source['name']}_raw.json"
    s3.download_file(BUCKET_NAME, source["raw_key"], local_raw_file)

    with open(local_raw_file, "r", encoding="utf-8") as f:
        tables = json.load(f)

    total_tables = len(tables)

    if total_tables == 0:
        print(f"{source['name']} içinde tablo yok!")
        continue

    start_index = (day_of_year * source["file_count"]) % total_tables

    for i in range(source["file_count"]):
        index = (start_index + i) % total_tables
        table = tables[index]

        filename = f"{source['name']}_{i+1}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(table, f, ensure_ascii=False)

        s3.upload_file(
            filename,
            BUCKET_NAME,
            f"{source['output_prefix']}{i+1}.json"
        )

    print(f"{source['name']} tamamlandı.")

print("Tüm modlar başarıyla üretildi ve yüklendi.")
