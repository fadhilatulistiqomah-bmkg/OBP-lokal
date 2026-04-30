import requests
import urllib3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
from datetime import date, datetime, timedelta
import sqlite3
import calendar # Ditambahkan untuk proses looping bulanan
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

# Hilangkan warning SSL (karena verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ⚡ SQLite Connection Setup
DB_PATH = "cuaca_ekstrem.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Test connection
try:
    cursor.execute("SELECT 1")
    conn.commit()
    print("[OK] Berhasil terhubung ke SQLite database")
except Exception as e:
    print(f"[ERROR] Gagal terhubung ke SQLite: {e}")

# Helper function untuk insert/update ke SQLite
def insert_to_sqlite(table_name, df, key_cols=None):
    """
    Insert/Update data ke SQLite dengan batch delete jika key_cols ada
    """
    if df is None or len(df) == 0:
        print(f"[INFO] Tidak ada data untuk disimpan ke {table_name}")
        return 0
    
    try:
        # Cek apakah tabel sudah ada
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        table_exists = cursor.fetchone() is not None
        
        if key_cols and len(key_cols) > 0 and table_exists:
            # Batch delete based on unique keys (hanya jika tabel sudah ada)
            unique_keys = df[key_cols].drop_duplicates()
            for _, row in unique_keys.iterrows():
                where_clause = " AND ".join([f"{col} = ?" for col in key_cols])
                values = tuple(row[col] for col in key_cols)
                cursor.execute(f"DELETE FROM {table_name} WHERE {where_clause}", values)
            conn.commit()
        
        # Insert records
        if len(df) > 0:
            # Jika tabel belum ada, gunakan 'replace', jika sudah ada gunakan 'append'
            if_exists = 'replace' if not table_exists else 'append'
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.commit()
            print(f"[OK] Disimpan {len(df)} baris ke tabel {table_name}")
            return len(df)
    except Exception as e:
        print(f"[ERROR] Error saat menyimpan ke {table_name}: {e}")
        return 0

# =======================================
# 3️⃣ Konfigurasi login & periode data
# =======================================
USERNAME = "pusmetbang"      # ganti dengan username BMKG Satu kamu
PASSWORD = "oprpusmetbang"   # ganti dengan password BMKG Satu kamu

# ⭐ GUNAKAN TANGGAL HARI INI ⭐
TANGGAL = datetime.now().strftime("%Y-%m-%d")  # Tanggal hari ini

# Validasi format tanggal
try:
    tanggal_obj = datetime.strptime(TANGGAL, "%Y-%m-%d").date()
    print(f"✅ Tanggal yang akan diproses: {TANGGAL}")
except ValueError:
    print("❌ Error: Format tanggal tidak valid. Gunakan format YYYY-MM-DD (misal: 2025-01-15)")
    exit()

# =======================================
# 4️⃣ Fungsi untuk ambil token
# =======================================
def ambil_token(username, password):
    url_login = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu/@login"
    payload = {"username": username, "password": password}
    response = requests.post(url_login, json=payload, verify=False)

    if response.status_code == 200:
        data = response.json()
        print("Respon Login:", data)  # debug isi respon
        token = data.get("token") or data.get("access_token")
        if token:
            print("✅ Token berhasil diambil")
            return token
        else:
            raise ValueError("❌ Token tidak ditemukan di response")
    else:
        raise ValueError(f"❌ Gagal login. Status code: {response.status_code}")

# =======================================
# 5️⃣ Fungsi untuk ambil data GTS (01 - 00 esok hari)
# =======================================
def ambil_data_gts(tanggal, token):
    tgl_akhir = datetime.strptime(tanggal, "%Y-%m-%d")
    tgl_awal = tgl_akhir - timedelta(days=1)
    
    # URL dan parameter disesuaikan dengan skrip Anda
    url = "https://bmkgsatu.bmkg.go.id/db/bmkgsatu//@search"
    params = {
        "type_name": "GTSMessage",
        "_metadata": "type_message,timestamp_data,timestamp_sent_data,station_wmo_id,sandi_gts,ttaaii,cccc,need_ftp",
        "_size": "10000",
        "type_message": "1",
        "timestamp_data__gte": f"{tgl_awal.strftime('%Y-%m-%d')}T01:00:00",
        "timestamp_data__lte": f"{tgl_akhir.strftime('%Y-%m-%d')}T00:59:59",
    }
    headers = {
        "authorization": f"Bearer {token}",
        "accept": "application/json"
    }
    response = requests.get(url, params=params, headers=headers, verify=False)

    if response.status_code == 200:
        print(f"✅ Data berhasil diambil untuk periode {params['timestamp_data__gte']} s/d {params['timestamp_data__lte']}")
        return response.json()
    else:
        raise ValueError(f"❌ Gagal mengambil data: {response.status_code} - {response.text}")

# =======================================
# 6️⃣ Jalankan proses untuk hari ini saja
# =======================================

print(f"\n{'='*30}")
print(f"🚀 MEMPROSES TANGGAL: {TANGGAL}")
print(f"{'='*30}")

# --- SCRIPT ASLI ANDA DIMULAI DI SINI (DENGAN INDENTASI) ---
try:
    token = ambil_token(USERNAME, PASSWORD)
    data_json = ambil_data_gts(TANGGAL, token)

    # pastikan ada data
    if "items" not in data_json or not data_json["items"]:
        print(f"⚠️ Data kosong untuk tanggal {TANGGAL}.")
        exit()

    # ambil hanya kolom yang diperlukan
    df = pd.DataFrame(data_json["items"])[[
        "timestamp_data",
        "timestamp_sent_data",
        "station_wmo_id",
        "ttaaii",
        "cccc",
        "sandi_gts"
    ]]

    print("✅ Data berhasil dimuat ke DataFrame")

    df['timestamp_data'] = pd.to_datetime(df['timestamp_data'], errors='coerce')
    df['timestamp_sent_data'] = pd.to_datetime(df['timestamp_sent_data'], errors='coerce')

    # Format ulang supaya semua ada microseconds
    df['timestamp_data'] = df['timestamp_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")
    df['timestamp_sent_data'] = df['timestamp_sent_data'].dt.strftime("%Y-%m-%dT%H:%M:%S")

    # Urutkan agar timestamp_sent_data terbaru berada di atas
    data_sorted = df.sort_values(['station_wmo_id','timestamp_data', 'timestamp_sent_data'], ascending=[True, True, False])

    # Ambil satu data per timestamp_data, yang paling baru dikirim
    data = data_sorted.drop_duplicates(subset=['station_wmo_id', 'timestamp_data'], keep='first')
