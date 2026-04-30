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

# ⭐ ATUR RENTANG TANGGAL DI SINI ⭐
TANGGAL_AWAL = "2025-12-31"   # Format: YYYY-MM-DD
TANGGAL_AKHIR = "2026-01-29"  # Format: YYYY-MM-DD

# Validasi format tanggal
try:
    tanggal_start = datetime.strptime(TANGGAL_AWAL, "%Y-%m-%d").date()
    tanggal_end = datetime.strptime(TANGGAL_AKHIR, "%Y-%m-%d").date()
    
    if tanggal_start > tanggal_end:
        print("❌ Error: Tanggal awal harus lebih kecil atau sama dengan tanggal akhir")
        exit()
    
    print(f"✅ Range tanggal valid: {TANGGAL_AWAL} s/d {TANGGAL_AKHIR}")
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
# 6️⃣ Jalankan proses secara looping per hari
# =======================================

# Buat list tanggal berdasarkan range yang diberikan
current_date = tanggal_start
list_tanggal = []

while current_date <= tanggal_end:
    list_tanggal.append(current_date.strftime("%Y-%m-%d"))
    current_date += timedelta(days=1)

print(f"\n📊 Total hari yang akan diproses: {len(list_tanggal)} hari")
print(f"Tanggal: {list_tanggal[0]} hingga {list_tanggal[-1]}\n")

# Loop untuk setiap hari
for TANGGAL in list_tanggal:
    
    print(f"\n{'='*30}")
    print(f"🚀 MEMPROSES TANGGAL: {TANGGAL}")
    print(f"{'='*30}")

    # --- SCRIPT ASLI ANDA DIMULAI DI SINI (DENGAN INDENTASI) ---
    try:
        token = ambil_token(USERNAME, PASSWORD)
        data_json = ambil_data_gts(TANGGAL, token)

        # pastikan ada data
        if "items" not in data_json or not data_json["items"]:
            print(f"⚠️ Data kosong untuk tanggal {TANGGAL}, lanjut ke hari berikutnya.")
            continue # Melompat ke iterasi (hari) selanjutnya

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

        data['timestamp_data'] = pd.to_datetime(data['timestamp_data'], errors='coerce')
        data['tanggal'] = data['timestamp_data'].dt.day
        data['jam'] = data['timestamp_data'].dt.hour

        def ambil_aaxx_beserta_isi(teks):
            """
            Ambil blok mulai dari AAXX (atau variasinya seperti AXX, AAAXX, AAX)
            hingga sebelum kelompok 333 (atau akhir pesan).
            Mengembalikan:
                - seksi01: potongan teks
                - false_aaxx: deskripsi kesalahan jika format AAXX tidak standar
            """
            if not isinstance(teks, str) or teks.strip() == "":
                return None, "teks kosong"

            # 1️⃣ Cari versi standar 'AAXX'
            match = re.search(r'(AAXX\s.*?\b333\b)', teks, re.DOTALL)
            if match:
                return match.group(1).strip(), ""  # tidak ada kesalahan

            # 2️⃣ Jika tidak ada, coba versi yang mirip tapi salah ketik (AXX, AAAXX, AAX)
            match_salah = re.search(r'\b(A{0,3}X{2,3})\s.*?\b333\b', teks, re.DOTALL)
            if match_salah:
                token = match_salah.group(1)
                if token != "AAXX":  # kalau bukan persis AAXX, berarti salah
                    return match_salah.group(0).strip(), "Tanda pengenal AAXX tidak valid"
                else:
                    return match_salah.group(0).strip(), ""

            # 3️⃣ Kalau tidak ditemukan sama sekali
            return None, "Tanda pengenal AAXX pada seksi 0 tidak valid"

        data[['seksi01', 'false_aaxx']] = data['sandi_gts'].apply(
            lambda x: pd.Series(ambil_aaxx_beserta_isi(x))
        )

        data['seksi01'] = (
        data['seksi01']
            .str.replace(r'CCA', '', regex=True)             # hapus semua CCA
            .str.replace(r'CCB', '', regex=True)             # hapus semua CCA
            .str.replace(r'\s+', ' ', regex=True)            # rapikan spasi
            .str.strip()                                     # hilangkan spasi awal/akhir
        )

        data['seksi0'] = pd.DataFrame(data.seksi01.astype(str).apply(lambda x: x[0:16] ))

        import re
        import pandas as pd

        def ambil_seksi1_dengan_toleransi(row):
            teks = str(row.get('seksi01', '')).strip()
            if not teks:
                return None

            # cari semua kandidat 5 digit angka (kemungkinan wmoid)
            kandidat_wmoid = re.findall(r'\b\d{5}\b', teks)

            # jika ada kandidat
            if kandidat_wmoid:
                for wmoid in kandidat_wmoid:
                    # hanya potong setelah wmoid yang valid (96/97/99)
                    if wmoid.startswith(('96', '97', '99')):
                        # ambil bagian setelah wmoid valid pertama
                        bagian_setelah = teks.split(wmoid, 1)[-1].strip()
                        return bagian_setelah

                # kalau tidak ada yang valid, tetap kembalikan bagian setelah kandidat pertama (fallback)
                wmoid_pertama = kandidat_wmoid[0]
                bagian_setelah = teks.split(wmoid_pertama, 1)[-1].strip()
                return bagian_setelah

            # kalau tidak ada angka 5 digit sama sekali
            return None

        # buat kolom seksi1
        data['seksi1'] = data.apply(ambil_seksi1_dengan_toleransi, axis=1)


        def ambil_yyggi(seksi0):
            """
            Ambil YYGGi dari potongan seksi0 (contoh: 'AAXX 14034')
            Return tuple (yy, gg, i)
            """
            if not isinstance(seksi0, str):
                return None, None, None

            match = re.search(r'A{1,3}X{2,3}\s+(\d{2})(\d{2})(\d)', seksi0)
            if match:
                yy = int(match.group(1))
                gg = int(match.group(2))
                i = int(match.group(3))
                return yy, gg, i
            return None, None, None



        def cek_yyggi_terhadap_timestamp(yy, gg, tanggal, jam):
            false_tanggal = ""
            false_jam = ""

            try:
                # kalau 'tanggal' adalah int (misal 30), ubah jadi datetime dummy
                if isinstance(tanggal, (int, float)):
                    tanggal_dt = datetime(2000, 1, int(tanggal)).date()
                else:
                    tanggal_dt = datetime.fromisoformat(str(tanggal)).date()

                # kalau jam adalah int (misal 6)
                if isinstance(jam, (int, float)):
                    jam_hour = int(jam)
                else:
                    jam_hour = int(str(jam).split(":")[0])

            except Exception:
                return "format timestamp tidak valid", "format timestamp tidak valid"

            # 1️⃣ Cek tanggal
            if yy != tanggal_dt.day:
                false_tanggal = "Sandi waktu (tanggal) pada seksi 0 tidak valid"

            # 2️⃣ Cek jam
            if gg != jam_hour:
                false_jam = "Sandi waktu (jam) pada seksi 0 tidak valid"

            return false_tanggal, false_jam


        def validasi_seksi0(row):
            yy, gg, i = ambil_yyggi(row['seksi0'])

            # pastikan hasil parsing jadi int kalau valid
            try:
                yy = int(float(yy)) if yy is not None and not pd.isna(yy) else None
            except:
                yy = None
            try:
                gg = int(float(gg)) if gg is not None and not pd.isna(gg) else None
            except:
                gg = None
            try:
                i = int(float(i)) if i is not None and not pd.isna(i) else None
            except:
                i = None

            false_tanggal, false_jam = cek_yyggi_terhadap_timestamp(
                yy, gg, row['tanggal'], row['jam']
            )

            return pd.Series({
                'yy': yy,
                'gg': gg,
                'i': i,
                'false_tanggal': false_tanggal,
                'false_jam': false_jam
            })


        # pemanggilan tetap sama
        data[['yy', 'gg', 'i', 'false_tanggal', 'false_jam']] = data.apply(validasi_seksi0, axis=1)

        def cek_false_waktu_format(seksi0):
            """
            Cek apakah sandi waktu pada seksi0 memiliki kesalahan format:
            - Mengandung huruf (misal 'Z')
            - Jumlah digit tidak sama dengan 5
            Return: pesan kesalahan atau None jika valid
            """
            if not isinstance(seksi0, str):
                return "Format seksi0 tidak valid"

            # cari bagian setelah AAXX
            match = re.search(r'A{1,3}X{2,3}\s+([A-Za-z0-9]+)', seksi0)
            if not match:
                return "Sandi waktu tidak ditemukan"

            kode_waktu = match.group(1).strip()

            # 1️⃣ Cek huruf (contoh: 31094Z)
            if re.search(r'[A-Za-z]', kode_waktu):
                return "Sandi waktu mengandung huruf (contoh: Z)"

            # 2️⃣ Cek panjang (harus tepat 5 digit angka)
            if not kode_waktu.isdigit() or len(kode_waktu) != 5:
                return f"Jumlah digit sandi waktu tidak valid (ditemukan {len(kode_waktu)} karakter)"

            # valid
            return None


        # Tambahkan kolom baru ke dataframe
        data['false_waktu'] = data['seksi0'].apply(cek_false_waktu_format)

        def validasi_wmoid(row):
            teks = str(row['seksi0'])
            station_id = str(row['station_wmo_id'])

            # cari pola IIiii setelah YYGGi (dengan opsional huruf seperti Z)
            match = re.search(r'A{1,3}X{2,3}\s*\d{5}\w?\s+(\d{5})', teks)
            if match:
                ii_iii = match.group(1)
                if ii_iii == station_id:
                    return ""  # ✅ valid
                else:
                    return "Nomor blok dan stasiun di seksi 0 tidak valid"
            else:
                return "Nomor blok dan stasiun di seksi 0 tidak valid"
        data['false_wmoid'] = data.apply(validasi_wmoid, axis=1)

        #data['iihvv'] = pd.DataFrame(data.seksi1.astype(str).apply(lambda x: x[0:5] if len(x) > 20 else None))
        data['iihvv'] = data['seksi1'].astype(str).apply(
            lambda x: x[0:5] if len(x) > 20 and x[0] in ['0', '1', '2', '3', '4'] else None
        )



        # --- Ambil nddff: 5 digit setelah iihvv ---
        def extract_nddff(teks, iihvv):
            try:
                if not isinstance(teks, str) or not isinstance(iihvv, str):
                    return None
                #pattern = re.escape(iihvv) + r'\s*(\d{5})(?=\s1)'  # cari 5 digit setelah iihvv, diikuti spasi1
                pattern = re.escape(iihvv) + r'\s*/?\s*(\d{4,5})(?=\s1)'
                match = re.search(pattern, teks)
                return match.group(1) if match else None
            except:
                return None

        data['nddff'] = data.apply(lambda row: extract_nddff(row['seksi1'], row['iihvv']), axis=1)

        data['nddff'] = data['nddff'].astype(str)

        # ambil arah (dd) dari posisi 1-3, kecepatan (ff) dari 3-5
        data['wd'] = data['nddff'].apply(lambda x: x[1:3] if len(x) >= 3 and x[1:3] != "//" else None)
        data['ws'] = data['nddff'].apply(lambda x: x[3:5] if len(x) >= 5 and x[3:5] != "//" else None)

        # tambahkan zero-padding agar dua digit
        data['wd'] = data['wd'].astype(str).str.zfill(2)
        data['ws'] = data['ws'].astype(str).str.zfill(2)

        # --- Fungsi interpretasi Arah Angin ---
        def interpret_wd(row):
            try:
                wd = int(row['wd'])
                ws = int(row['ws'])
                
                # Kasus khusus: tidak ada angin
                if wd == 0 and ws == 0:
                    return None  
                
                # Kasus arah utara (ada angin tapi wd = 00)
                if wd == 0 and ws > 0:
                    return 0  
                
                # Normal: kode wd dikali 10 derajat
                if 1 <= wd <= 36:
                    return wd * 10
                
                return None
            except:
                return None


        # --- Fungsi interpretasi Kecepatan Angin ---
        def interpret_ws(ws):
            try:
                ws = int(ws)
                if ws == 0:
                    return None
                elif 1 <= ws <= 99:
                    return ws
                else:
                    return None
            except:
                return None


        # --- Fungsi interpretasi Gale (angin kencang) ---
        def interpret_gale(ws):
            try:
                ws = int(ws)
                if ws >= 30:
                    return ws
                else:
                    return None
            except:
                return None


        # --- Terapkan ke DataFrame ---
        data['Arah_angin'] = data.apply(interpret_wd, axis=1)
        data['Kecepatan_angin'] = data['ws'].apply(interpret_ws)
        data['Gale'] = data['ws'].apply(interpret_gale)

        def ambil_seksi1_1(teks):
            teks = str(teks).replace('\n', ' ').strip()
            match = re.search(r'(1[0-9/]{4}\s2[0-9/]{4}.*)', teks)
            if match:
                return match.group(1).strip()
            return None
        data['seksi1_1'] = data['seksi1'].apply(ambil_seksi1_1)

        # Loop otomatis dari sandi1 sampai sandi8
        for i in range(1, 9):
            # Regex: angka pertama = nomor sandi, 4 karakter berikut boleh angka atau '/'
            regex = fr'({i}[0-9/]{{4}})'
            data[f'sandi{i}'] = data['seksi1_1'].astype(str).str.extract(regex, expand=False)

        def interpret_ttt(sandi1):
            try:
                sandi1 = str(sandi1).strip()
                if len(sandi1) < 5:
                    return None
                
                # Ambil substring TTT (3 digit terakhir dari karakter ke-2 sampai ke-4)
                ttt_str = sandi1[2:5]
                
                # Pastikan cukup panjang
                if len(ttt_str) != 3:
                    return None
                
                # Angka kedua dari sandi1 menentukan tanda (0 = positif, 1 = negatif)
                sign_digit = sandi1[1]
                value = int(ttt_str)
                suhu = value / 10
                
                if sign_digit == '0':
                    return suhu
                elif sign_digit == '1':
                    return -suhu
                else:
                    return None
            except:
                return None

        data['Temperatur'] = data['sandi1'].apply(interpret_ttt)

        def interpret_tdtdtd(sandi2):
            try:
                sandi2 = str(sandi2).strip()
                if len(sandi2) < 5:
                    return None
                
                # Ambil 3 digit terakhir sebagai TdTdTd
                tdtdtd_str = sandi2[2:5]
                
                if len(tdtdtd_str) != 3:
                    return None
                
                # Angka kedua dari sandi2 menentukan tanda (0 = positif, 1 = negatif)
                sign_digit = sandi2[1]
                value = int(tdtdtd_str)
                suhu = value / 10
                
                if sign_digit == '0':
                    return suhu
                elif sign_digit == '1':
                    return -suhu
                else:
                    return None
            except:
                return None

        data['Dew_Point'] = data['sandi2'].apply(interpret_tdtdtd)       

        def interpret_qfe(sandi3):
            try:
                sandi3 = str(sandi3).strip()
                if len(sandi3) < 5:
                    return None

                # Ambil 4 digit PoPoPoPo (mulai dari karakter ke-2)
                popopopo = sandi3[1:5]
                qfe_int = int(popopopo)
                qfe_str = popopopo.zfill(4)

                # Aturan konversi
                if qfe_str.startswith('0'):
                    return (qfe_int / 10) + 1000
                else:
                    return qfe_int / 10
            except:
                return None

        data['Tekanan_Permukaan'] = data['sandi3'].apply(interpret_qfe)

        def interpret_qff(sandi4):
            try:
                sandi4 = str(sandi4).strip()
                if len(sandi4) < 5:
                    return None

                # Ambil 4 digit PPPP (mulai dari karakter ke-2)
                pppp = sandi4[1:5]
                qff_int = int(pppp)
                qff_str = pppp.zfill(4)

                # Aturan konversi tekanan
                if qff_str.startswith('0'):
                    return (qff_int / 10) + 1000
                else:
                    return qff_int / 10
            except:
                return None

        data['Tekanan_Laut'] = data['sandi4'].apply(interpret_qff)      

        def interpret_ppp(sandi5):
            try:
                sandi5 = str(sandi5).strip()
                if len(sandi5) < 5:
                    return None

                # Ambil 3 digit PPP (mulai dari karakter ke-3)
                ppp = sandi5[2:5]
                ppp_str = ppp.zfill(3)
                ppp_int = int(ppp_str)

                # Interpretasi nilai selisih tekanan
                return ppp_int / 10
            except:
                return None

        data['Selisih_Tekanan'] = data['sandi5'].apply(interpret_ppp)

        def interpret_rain(sandi6):
            try:
                sandi6 = str(sandi6).strip()
                
                # Validasi format: harus diawali '6' dan diakhiri '4'
                if not (len(sandi6) >= 5 and sandi6[0] == '6' and sandi6[4] == '4'):
                    return None
                
                # Ambil 3 digit curah hujan (ch)
                ch_str = sandi6[1:4].zfill(3)
                ch = int(ch_str)
                
                # Interpretasi sesuai kode
                if 1 <= ch < 990:
                    return ch               # Curah hujan dalam 0.1 mm
                elif ch == 990:
                    return None             # Tidak ada data
                elif 991 <= ch <= 999:
                    return (ch - 990) / 10  # Dalam satuan mm (kode 991–999)
                else:
                    return None
            except:
                return None

        data['Curah_Hujan'] = data['sandi6'].apply(interpret_rain)


        def interpret_heavy_rain(ch):
            try:
                if ch is None:
                    return None
                    ch = float(ch)
                if ch >= 50:
                    return ch
                    return None
            except:
                return None

        data['Heavy_Rain'] = data['Curah_Hujan'].apply(interpret_heavy_rain)     

        data['ww'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[1:3] if len(x) >= 3 and x[0]=='7' else None))

        data['ww'] = data['ww'].astype(str).str.zfill(2)
        mapping_df = pd.read_excel('ww.xlsx')
        mapping_df['kode'] = mapping_df['kode'].astype(str).str.zfill(2)
        mapping_dict = mapping_df.set_index('kode')['interpretasi'].to_dict()
        data['ww_interpretasi'] = data['ww'].map(mapping_dict)

        data['W1'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[3:4] if len(x) >= 4 and x[0]=='7' else None))

        def interpret_w(W1):
            try:
                W1 = int(W1)
                if W1 == 0:
                    return 'Awan menutupi langit setengah atau kurang selama jangka waktu yang ditentukan'
                elif W1 == 1 :
                    return 'Awan menutupi langit lebih dari setengah selama sebagian dari jangka waktu yang ditetaokan dan setengah atau kurang selama sebagian dari jangka waktu itu'
                elif W1 == 2 :
                    return 'Awan menutupi langit lebih dari setengah selama jangka waktu yang ditetapkan'
                elif W1 == 3 :
                    return 'Badai pasir, badai debu, atau salju hembus'
                elif W1 == 4 :
                    return 'Kabut atau kekaburan tebal'
                elif W1 == 5 :
                    return 'Drizzlo'
                elif W1 == 6 :
                    return 'Hujan'
                elif W1 == 7 :
                    return 'Salju atau hujan bercampur salju'
                elif W1 == 8 :
                    return 'Hujan tiba-tiba (Showers)'
                elif W1 == 9 :
                    return 'Badai guntur disertai endapan atau tidak disertai endapan'
            except:
                return None

        data['W1_interpretasi'] = data['W1'].apply(interpret_w)
        data['W2'] = pd.DataFrame(data.sandi7.astype(str).apply(lambda x: x[4:5] if len(x) >= 5 and x[0]=='7' else None))
        data['W2_interpretasi'] = data['W2'].apply(interpret_w)

        data['Nh'] = pd.DataFrame(data.sandi8.astype(str).apply(lambda x: x[1:2] if len(x) >= 3 and x[0]=='8' else None))

        data['Nh'] = data['Nh'].astype(str).str.zfill(1)

        def interpret_cloudL(C):
            try:
                C = int(C)
                if C == 0:
                    return 'Tidak ada awan'
                elif C == 1 :
                    return 'Cumulus humilis atau fracto cumulus atau kedua-duanya'
                elif C == 2 :
                    return 'Cumulus mediocris atau congestus, disertai atau tidak disertai fracto cumulus atau humilis atau strato cumulus, dengan tinggi dasar sama'
                elif C == 3 :
                    return 'Cumulunimbus tanpa landasan, disertai atau tidak disertai cumulus, strato cumulus atau stratus'
                elif C == 4 :
                    return 'Stratocumulus yang terjadi dari bentangan cumulus'
                elif C == 5 :
                    return 'Stratocumulus yang tidak terjadi dari bentangan cumulus'
                elif C == 6 :
                    return 'Stratus'
                elif C == 7 :
                    return 'Fraktotratus atau fraktocumulus yang menyertai cuaca buruk, biasanya di bawah As atau Ns '
                elif C == 8 :
                    return 'Cumulus dan stratocumulus yang tidak terjadi dari bentangan cumulus, dengan tinggi dasar berlainan'
                elif C == 9 :
                    return 'Cumulunimbus, biasanya berlandaskan disertai cumulus, stratocumulus, stratus, cumulunimbus yang tidak berlandaskan'
            except:
                return 'Tidak terlihat'
            

        def interpret_cloudM(C):
            try:
                C = int(C)
                if C == 0:
                    return 'Tidak ada awan'
                elif C == 1 :
                    return 'Altostratus tipis'
                elif C == 2 :
                    return 'Altostratus tebal atau nimbostratus'
                elif C == 3 :
                    return 'Altocumulus tipis dalam suatu lapisan '
                elif C == 4 :
                    return 'Altocumulus tipis berbentuk terpisah-pisah, sering sekali berbentuk lensa, terus berubah dan terdapat pada satu lapisan atau lebih'
                elif C == 5 :
                    return 'Altocumulus tipis berbentuk pias-pias atau beberapa lapisan altocumulus tipis atau tebal dalam keadaan bertambah '
                elif C == 6 :
                    return 'Altocumulus yang terjadi dari bentangan cumulus'
                elif C == 7 :
                    return 'Altocumulus tipis atau tebal dalam beberapa lapisan, atau satu lapisan altocumulus tebal, tidak dalam keadaan bertambah, atau altocumulus serta altostratus atau nimbostratus'
                elif C == 8 :
                    return 'Altocumulus castellatus (bertanduk) atau berbentuk bayangan bintik '
                elif C == 9 :
                    return 'Altocumulus dalam berbagai-bagai lapisan dan bentuk, kelihatan tidak teratur'
            except:
                return 'Tidak terlihat'

        def interpret_cloudH(C):
            try:
                C = int(C)
                if C == 0:
                    return 'Tidak ada awan'
                elif C == 1 :
                    return 'Cirrus halus seperti bulu ayam, tidak dalam keadaan bertambah '
                elif C == 2 :
                    return 'Cirrus padat, terpisah-pisah atau masa yang kusut, biasanya tidak bertambah, kadang-kadang seperti sisa-sisa landasan cumulunimbus'
                elif C == 3 :
                    return 'Cirrus padat, terjadi dari landasan cumulunimbus '
                elif C == 4 :
                    return 'Cirrus halus dalam bentuk koma, atau bulu ayam, menjadi lebih padat atau bertambah'
                elif C == 5 :
                    return 'Cirrus dan cirrostratus, cirrostratus sendirian, dalam keadaan bertambah akan tetapi lapisan tidak mencapai ketinggian 45o di atas cakrawala'
                elif C == 6 :
                    return 'Cirrus dan cirrostratus, atau cirrostratus sendirian, menjadi lebih padat dan dalam keadaan bertambah, lapisan meluas lebih dari 45o di atas cakrawala akan tetapi langit tidak tertutup semuanya '
                elif C == 7 :
                    return 'Lapisan cirrostratus yang menutupi seluruh langit '
                elif C == 8 :
                    return 'Cirrostratus yang tidak menutupi seluruh langit dan tidak bertambah'
                elif C == 9 :
                    return 'Cirrocumulus, cirrocumulus yang terbanyak dengan sedikit cirrus dan / atau cirrostratus'
            except:
                return 'Tidak terlihat'
            

        def awan_rendah(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('8') and len(t) >= 3:
                    return interpret_cloudL(t[2])  # ambil angka ke-3 dan interpretasikan
            return None
        data['CL'] = data['sandi8'].apply(awan_rendah)

        def awan_menengah(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('8') and len(t) >= 3:
                    return interpret_cloudM(t[3])  # ambil angka ke-3 dan interpretasikan
            return None
        data['CM'] = data['sandi8'].apply(awan_menengah)

        def awan_tinggi(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('8') and len(t) >= 3:
                    return interpret_cloudH(t[4])  # ambil angka ke-3 dan interpretasikan
            return None
        data['CH'] = data['sandi8'].apply(awan_tinggi) 

        # def ambil_setelah_333(teks):
        #     if not isinstance(teks, str):
        #         return None
        #     match = re.search(r'333\s+(.*?)=', teks, re.DOTALL)
        #     return match.group(1).strip() if match else None

        # data['seksi3'] = data['sandi_gts'].apply(ambil_setelah_333)
        def ambil_setelah_333(teks):
            if not isinstance(teks, str):
                return None
            # pastikan sebelum 333 adalah spasi atau awal baris
            match = re.search(r'(?:^|\s)333\s+(.*)', teks, re.DOTALL)
            return match.group(1).strip() if match else None

        data['seksi3'] = data['sandi_gts'].apply(ambil_setelah_333)
        data['seksi3'] = data['seksi3'].str.replace(r'\s+', ' ', regex=True).str.strip()

        # # --- Ambil sandi 2 (TnTnTn) ---
        # def ambil_sandi1(teks):
        #     if not isinstance(teks, str):
        #         return None
        #     tokens = teks.split()
        #     for t in tokens:
        #         if t.startswith('1'):
        #             return t
        #     return None
        
        def ambil_sandi1(teks):
            if not isinstance(teks, str):
                return None
            
            tokens = teks.split()
            
            # Periksa apakah ada token (teks tidak kosong)
            if not tokens:
                return None
                
            # Langsung periksa token PERTAMA (indeks 0)
            token_pertama = tokens[0]
            
            if token_pertama.startswith('1'):
                return token_pertama
                
            # Jika token pertama tidak berawalan '1', kembalikan None
            return None

        data['sn1'] = data['seksi3'].apply(ambil_sandi1)

        #data['TxTxTx'] = pd.DataFrame(data.sn1.astype(str).apply(lambda x: x[2:5] if len(x) >= 4  else None))

        def interpret_tmax(sn1max):
            try:
                sn1max = str(sn1max).strip()
                if len(sn1max) < 5 or sn1max[0] != '1':
                    return None
                
                # Ambil 3 digit suhu maksimum (TxTxTx)
                tx_str = sn1max[2:5]
                
                # Ambil tanda (0 = positif, 1 = negatif)
                sign_digit = sn1max[1]
                value = int(tx_str)
                suhu = value / 10

                if sign_digit == '0':
                    return suhu
                elif sign_digit == '1':
                    return -suhu
                else:
                    return None
            except:
                return None

        data['Tmax'] = data['sn1'].apply(interpret_tmax)

        # # --- Ambil sandi 2 (TnTnTn) ---
        # def ambil_sandi2(teks):
        #     if not isinstance(teks, str):
        #         return None
        #     tokens = teks.split()
        #     for t in tokens:
        #         if t.startswith('2'):
        #             return t
        #     return None

        def ambil_sandi2(teks):
            if not isinstance(teks, str):
                return None
            
            tokens = teks.split()
            
            # Periksa apakah ada token (teks tidak kosong)
            if not tokens:
                return None
                
            # Langsung periksa token PERTAMA (indeks 0)
            token_pertama = tokens[0]
            
            if token_pertama.startswith('2'):
                return token_pertama
                
            # Jika token pertama tidak berawalan '2', kembalikan None
            return None

        data['sn2'] = data['seksi3'].apply(ambil_sandi2)

        def interpret_tmin(sn2):
            try:
                sn2 = str(sn2).strip()
                if len(sn2) < 5 or sn2[0] != '2':
                    return None

                # Ambil tiga digit suhu minimum (TnTnTn)
                tn_str = sn2[2:5]

                # Ambil tanda (0 = positif, 1 = negatif)
                sign_digit = sn2[1]
                value = int(tn_str)
                suhu = value / 10

                if sign_digit == '0':
                    return suhu
                elif sign_digit == '1':
                    return -suhu
                else:
                    return None
            except:
                return None

        data['Tmin'] = data['sn2'].apply(interpret_tmin)

        def ambil_sandi53(teks):
            teks = str(teks).replace('\n', ' ')
            match = re.search(r'2[0-9/]{4}\s(5[0-46-9/][0-9/]{3})', teks)
            if match:
                return match.group(1)
            return None

        data['sandi53'] = data['seksi3'].apply(ambil_sandi53)

        # Ekstrak EEE dari sandi 5EEEiE
        data['Evaporasi'] = data['sandi53'].apply(
            lambda x: x[1:4] if isinstance(x, str) and len(x) >= 5 and x.startswith('5') else None
        )

        # Konversi ke numerik aman
        data['Evaporasi'] = pd.to_numeric(data['Evaporasi'], errors='coerce')

        # Skala (dibagi 10)
        data['Evaporasi'] = data['Evaporasi'] / 10

        # --- Ambil sandi 55 ---
        def ambil_sandi55(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('55'):
                    return t
            return None

        data['sandi55'] = data['seksi3'].apply(ambil_sandi55)

        def interpret_lama_penyinaran(sandi55):
            try:
                # Validasi awal: harus string, diawali '55', dan panjang minimal 4
                if not (isinstance(sandi55, str) and sandi55.startswith('55') and len(sandi55) >= 4):
                    return None
                
                # Ambil 3 digit SSS (lama penyinaran)
                sss_str = sandi55[2:5]
                sss = pd.to_numeric(sss_str, errors='coerce')
                if pd.isna(sss):
                    return None

                # Konversi ke jam dan menit
                jam_desimal = sss / 10
                jam = int(jam_desimal)
                menit = int(round((jam_desimal - jam) * 60))
                return f"{jam} jam {menit} menit"
            except:
                return None

        data['Lama_Penyinaran'] = data['sandi55'].apply(interpret_lama_penyinaran)

        # --- Ambil sandi 56 ---
        def ambil_sandi56(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('56'):
                    return t
            return None

        data['sandi56'] = data['seksi3'].apply(ambil_sandi56)

        def interpret_cloudmove(C):
            try:
                C = int(C)
                if C == 0:
                    return 'Awan tidak bergerak'
                elif C == 1 :
                    return 'NE'
                elif C == 2 :
                    return 'E'
                elif C == 3 :
                    return 'SE'
                elif C == 4 :
                    return 'S'
                elif C == 5 :
                    return 'SW'
                elif C == 6 :
                    return 'W'
                elif C == 7 :
                    return 'NW'
                elif C == 8 :
                    return 'N'
                elif C == 9 :
                    return 'Tidak diketahui'
            except:
                return None
            
            
        def ambil_arah_awan_L(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('56') and len(t) >= 3:
                    return interpret_cloudmove(t[2])
            return None

        def ambil_arah_awan_M(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('56') and len(t) >= 4:  # ubah dari >=3 ke >=4
                    return interpret_cloudmove(t[3])
            return None

        def ambil_arah_awan_H(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('56') and len(t) >= 5:  # ubah dari >=3 ke >=5
                    return interpret_cloudmove(t[4])
            return None

        data['DL'] = data['sandi56'].apply(ambil_arah_awan_L)

        data['DM'] = data['sandi56'].apply(ambil_arah_awan_M)

        data['DH'] = data['sandi56'].apply(ambil_arah_awan_H)



        # --- Ambil sandi 57 ---
        def ambil_sandi57(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('57'):
                    return t
            return None

        data['sandi57'] = data['seksi3'].apply(ambil_sandi57)

        def awan_L(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('57') and len(t) >= 3:
                    return interpret_cloudL(t[2])  # ambil angka ke-3 dan interpretasikan
            return None

        data['Awan_Rendah'] = data['sandi57'].apply(awan_L)

        def arah_sebenarnya(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('57') and len(t) >= 4:
                    return interpret_cloudmove(t[3])  # ambil angka ke-3 dan interpretasikan
            return None

        data['Arah_Sebenarnya'] = data['sandi57'].apply(arah_sebenarnya)

        def interpretasi_elevasi(C):
            try:
                C = int(C)
                if C == 0:
                    return 'Puncak awan tidak terlihat'
                elif C == 1 :
                    return '45°'
                elif C == 2 :
                    return '30°'
                elif C == 3 :
                    return '20°'
                elif C == 4 :
                    return '15°'
                elif C == 5 :
                    return '12°'
                elif C == 6 :
                    return '9°'
                elif C == 7 :
                    return '7°'
                elif C == 8 :
                    return '6°'
                elif C == 9 :
                    return '5°'
            except:
                return None

        def sudut_elevasi(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('57') and len(t) >= 5:
                    return interpretasi_elevasi(t[4])  # ambil angka ke-3 dan interpretasikan
            return None

        data['Elevasi'] = data['sandi57'].apply(sudut_elevasi)

        def ambil_sandi58(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('58'):
                    return t
            return None

        data['sandi58'] = data['seksi3'].apply(ambil_sandi58)

        def ambil_sandi59(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('59'):
                    return t
            return None

        data['sandi59'] = data['seksi3'].apply(ambil_sandi59)



        # --- Ambil sandi 63 (curah hujan) ---
        def ambil_sandi63(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('6'):
                    return t
            return None

        data['sandi63'] = data['seksi3'].apply(ambil_sandi63)

        # --- Interpretasi curah hujan langsung dari sandi63 ---
        def interpret_rain(x):
            try:
                # Pastikan x adalah string dengan format 6xxx
                if isinstance(x, str) and len(x) >= 5 and x.startswith('6'):
                    ch = int(x[1:4])  # ambil 3 digit di tengah
                    
                    if 1 <= ch < 990:
                        return ch
                    elif ch == 990:
                        return None
                    elif 991 <= ch <= 999:
                        return (ch - 990) / 10
            except:
                return None
            return None

        data['Curah_Hujan_Jam'] = data['sandi63'].apply(interpret_rain)

        # --- Ambil sandi 63 (curah hujan) ---
        def ambil_sandi83(teks):
            if not isinstance(teks, str):
                return None
            tokens = teks.split()
            for t in tokens:
                if t.startswith('8'):
                    return t
            return None

        data['sandi83'] = data['seksi3'].apply(ambil_sandi83)

        data['Nh3'] = pd.DataFrame(data.sandi83.astype(str).apply(lambda x: x[1:2] if len(x) >= 3 and x[0]=='8' else None))

        data['Nh3'] = data['Nh3'].astype(str).str.zfill(1)

        def interpret_cloud(x):
            try:
                # Pastikan string dan formatnya diawali 8
                if isinstance(x, str) and len(x) >= 4 and x[0] == '8':
                    C = int(x[2])
                    return {
                        0: 'Cirrus (Ci)',
                        1: 'Cirrocumulus (Cc)',
                        2: 'Cirrostratus (Cs)',
                        3: 'Altocumulus (Ac)',
                        4: 'Altostratus (As)',
                        5: 'Nimbostratus (Ns)',
                        6: 'Stratocumulus (Sc)',
                        7: 'Stratus (St)',
                        8: 'Cumulus (Cu)',
                        9: 'Cumulonimbus (Cb)'
                    }.get(C, None)
            except:
                return None
            return None

        data['C_interpretasi'] = data['sandi83'].apply(interpret_cloud)

        # --- Bersihkan 'None' string jadi NaN ---
        data.replace('None', np.nan, inplace=True)


        def cek_false_ir(row):
            """
            Aturan yang diterapkan (lebih ketat):
            - ir = '0' -> harus ada sandi6 AND sandi63
                -> error jika salah satu/dua-duanya hilang
            - ir = '1' -> harus ada sandi6 AND sandi63 harus TIDAK ADA
                -> error jika sandi6 tidak ada OR sandi63 ada
            - ir = '2' -> harus ada sandi63 AND sandi6 harus TIDAK ADA
                -> error jika sandi63 tidak ada OR sandi6 ada
            - ir = '3' or '4' -> sandi6 dan sandi63 harus TIDAK ADA
                -> error jika salah satu/dua-duanya ada
            """
            # iihvv = str(row.get('iihvv', ''))
            # Kode perbaikan
            raw_iihvv = row.get('iihvv')
            # Jika nilainya None/NaN, anggap string kosong. Jika tidak, baru konversi ke string.
            iihvv = "" if pd.isna(raw_iihvv) else str(raw_iihvv)
            sandi6 = row.get('sandi6')
            sandi63 = row.get('sandi63')

            # Ambil digit pertama sebagai Ir jika ada digit di iihvv
            ir = iihvv[0] if len(iihvv) > 0 and iihvv[0].isdigit() else None

            # normalisasi keberadaan (True jika ada isi non-empty)
            ada_sandi6 = pd.notna(sandi6) and str(sandi6).strip() != ""
            ada_sandi63 = pd.notna(sandi63) and str(sandi63).strip() != ""

            errors = []

            if ir is None:
                errors.append("Ir tidak tersedia/tidak valid")
            else:
                if ir == '0':
                    if not ada_sandi6 and not ada_sandi63:
                        errors.append("Sandi 6RRRtR seksi 1 dan 3 tidak ditemukan")
                    elif not ada_sandi6:
                        errors.append("Sandi 6RRRtR seksi 1 tidak ditemukan (dibutuhkan untuk Ir=0)")
                    elif not ada_sandi63:
                        errors.append("Sandi 6RRRtR seksi 3 tidak ditemukan (dibutuhkan untuk Ir=0)")
                elif ir == '1':
                    if not ada_sandi6:
                        errors.append("Sandi 6RRRtR seksi 1 tidak ditemukan (dibutuhkan untuk Ir=1)")
                    if ada_sandi63:
                        errors.append("Sandi 6RRRtR seksi 3 seharusnya tidak ada untuk Ir=1")
                elif ir == '2':
                    if not ada_sandi63:
                        errors.append("Sandi 6RRRtR seksi 3 tidak ditemukan (dibutuhkan untuk Ir=2)")
                    if ada_sandi6:
                        errors.append("Sandi 6RRRtR seksi 1 seharusnya tidak ada untuk Ir=2")
                elif ir in ('3', '4'):
                    if ada_sandi6 or ada_sandi63:
                        errors.append("Sandi 6RRRtR seksi 1 atau 3 seharusnya tidak ada untuk Ir=3/4")
                else:
                    errors.append(f"Ir tidak dikenali: '{ir}'")

            # Gabungkan pesan; None berarti valid
            false_ir_msg = None if len(errors) == 0 else "; ".join(errors)
            #valid_flag = (false_ir_msg is None)

            return pd.Series({
                "false_ir": false_ir_msg,
                # "valid_ir": valid_flag,
                # "ir_value": ir
            })

        # Terapkan ke dataframe (akan menambah tiga kolom)
        data[['false_ir']] = data.apply(cek_false_ir, axis=1)

        def cek_false_ix(row):
            """
            Validasi Ix terhadap keberadaan sandi7.
            Aturan:
            - Ix = 1 atau 4 → sandi7 harus ADA
            - Ix = 2, 3, 5, atau 6 → sandi7 harus TIDAK ADA
            """
            iihvv = str(row.get('iihvv', ''))
            sandi7 = row.get('sandi7')

            # Ambil digit kedua dari iihvv (Ix)
            ix = iihvv[1] if len(iihvv) > 1 and iihvv[1].isdigit() else None

            # Cek keberadaan sandi7
            ada_sandi7 = pd.notna(sandi7) and str(sandi7).strip() != ""

            errors = []

            if ix is None:
                errors.append("Ix tidak tersedia/tidak valid")
            else:
                if ix in ['1', '4']:
                    if not ada_sandi7:
                        errors.append("Sandi 7 tidak ditemukan (dibutuhkan untuk Ix=1/4)")
                elif ix in ['2', '3', '5', '6']:
                    if ada_sandi7:
                        errors.append("Sandi 7 seharusnya tidak ada untuk Ix=2/3/5/6")
                else:
                    errors.append(f"Ix tidak dikenali: '{ix}'")

            false_ix_msg = None if len(errors) == 0 else "; ".join(errors)
            # valid_ix = (false_ix_msg is None)

            return pd.Series({
                "false_ix": false_ix_msg,
                # "valid_ix": valid_ix,
                # "ix_value": ix
            })

        # Terapkan ke dataframe utama
        data[['false_ix']] = data.apply(cek_false_ix, axis=1)

        def cek_false_nddff(row):
            """
            Menandai kesalahan jika kolom 'nddff' kosong.
            """
            nddff = row.get('nddff')

            if pd.isna(nddff) or str(nddff).strip() == "":
                return "Sandi nddff tidak ditemukan di seksi 1"
            else:
                return None  # valid

        # Buat kolom baru false_nddff
        data['false_nddff'] = data.apply(cek_false_nddff, axis=1)

        def cek_false_sandi5(row):
            """
            Validasi sandi5 terhadap jam laporan SYNOP.
            - Harus ada di jam: 00, 03, 06, 09, 12, 15, 18, 21
            - Tidak boleh ada di jam lainnya.
            """
            jam = row.get('jam')
            sandi5 = row.get('sandi5')

            # pastikan jam dalam bentuk integer
            try:
                jam_int = int(str(jam).split(':')[0])  # misal jam="03:00:00"
            except Exception:
                return "Format jam tidak valid"

            # Daftar jam wajib laporan
            jam_wajib = [0, 3, 6, 9, 12, 15, 18, 21]

            # Cek keberadaan sandi5
            ada_sandi5 = pd.notna(sandi5) and str(sandi5).strip() != ""

            # Logika utama
            if jam_int in jam_wajib:
                if not ada_sandi5:
                    return f"Sandi 5 seharusnya ada pada jam {jam_int:02d}"
            else:
                if ada_sandi5:
                    return f"Sandi 5 tidak boleh ada pada jam {jam_int:02d}"

            # Jika semua valid
            return None

        # Terapkan ke dataframe
        data['false_sandi5'] = data.apply(cek_false_sandi5, axis=1)

        def cek_false_sn1(row):
            """
            Validasi sandi sn1 terhadap jam laporan.
            - Harus ADA pada jam 12
            - Tidak boleh ada pada jam lain
            """
            jam = row.get('jam')
            sn1 = row.get('sn1')

            # Pastikan jam bisa dibaca sebagai integer
            try:
                jam_int = int(str(jam).split(':')[0])  # kalau format jam "12:00:00"
            except Exception:
                return "Format jam tidak valid"

            ada_sn1 = pd.notna(sn1) and str(sn1).strip() != ""

            if jam_int == 12:
                if not ada_sn1:
                    return "Sandi sn1 seharusnya ada pada jam 12"
            else:
                if ada_sn1:
                    return f"Sandi sn1 tidak boleh ada pada jam {jam_int:02d}"

            return None  # valid

        # Tambahkan kolom ke dataframe
        data['false_sn1'] = data.apply(cek_false_sn1, axis=1)

        def cek_false_sn2(row):
            """
            Validasi sandi sn2 terhadap jam laporan.
            - Harus ADA pada jam 00
            - Tidak boleh ada pada jam lain
            """
            jam = row.get('jam')
            sn2 = row.get('sn2')

            # Pastikan jam bisa dibaca sebagai integer
            try:
                jam_int = int(str(jam).split(':')[0])  # kalau format jam "00:00:00"
            except Exception:
                return "Format jam tidak valid"

            ada_sn2 = pd.notna(sn2) and str(sn2).strip() != ""

            if jam_int == 00:
                if not ada_sn2:
                    return "Sandi sn2 seharusnya ada pada jam 00"
            else:
                if ada_sn2:
                    return f"Sandi sn2 tidak boleh ada pada jam {jam_int:02d}"

            return None  # valid

        # Tambahkan kolom ke dataframe
        data['false_sn2'] = data.apply(cek_false_sn2, axis=1)

        def cek_false_sandi53(row):
            """
            Validasi sandi53 terhadap jam laporan.
            - Harus ADA pada jam 00
            - Tidak boleh ada pada jam lain
            """
            jam = row.get('jam')
            sandi53 = row.get('sandi53')

            try:
                jam_int = int(str(jam).split(':')[0])  # dukung format "00:00:00"
            except Exception:
                return "Format jam tidak valid"

            ada_sandi53 = pd.notna(sandi53) and str(sandi53).strip() != ""

            if jam_int == 0:
                if not ada_sandi53:
                    return "Sandi 53 seharusnya ada pada jam 00"
            else:
                if ada_sandi53:
                    return f"Sandi 53 tidak boleh ada pada jam {jam_int:02d}"

            return None


        def cek_false_sandi55(row):
            """
            Validasi sandi55 terhadap jam laporan.
            - Harus ADA pada jam 00
            - Tidak boleh ada pada jam lain
            """
            jam = row.get('jam')
            sandi55 = row.get('sandi55')

            try:
                jam_int = int(str(jam).split(':')[0])
            except Exception:
                return "Format jam tidak valid"

            ada_sandi55 = pd.notna(sandi55) and str(sandi55).strip() != ""

            if jam_int == 0:
                if not ada_sandi55:
                    return "Sandi 55 seharusnya ada pada jam 00"
            else:
                if ada_sandi55:
                    return f"Sandi 55 tidak boleh ada pada jam {jam_int:02d}"

            return None


        # Tambahkan kolom hasil validasi
        data['false_sandi53'] = data.apply(cek_false_sandi53, axis=1)
        data['false_sandi55'] = data.apply(cek_false_sandi55, axis=1)

        def cek_false_sandi58_59(row):
            """
            Validasi kombinasi sandi58 dan sandi59.
            Aturan:
            - Hanya boleh dilaporkan pada jam 00.
            - Pada jam 00 -> harus ADA salah satu (58 atau 59), tidak boleh keduanya, tidak boleh kosong.
            - Pada jam lain -> keduanya harus TIDAK ADA.
            """
            jam = row.get('jam')
            sandi58 = row.get('sandi58')
            sandi59 = row.get('sandi59')

            # pastikan jam bisa dibaca
            try:
                jam_int = int(str(jam).split(':')[0])
            except Exception:
                return "Format jam tidak valid"

            # cek keberadaan
            ada_58 = pd.notna(sandi58) and str(sandi58).strip() != ""
            ada_59 = pd.notna(sandi59) and str(sandi59).strip() != ""

            # logika validasi
            if jam_int in (0,12):
                # hanya boleh salah satu
                if ada_58 and ada_59:
                    return "Sandi 58 dan 59 tidak boleh muncul bersamaan pada jam 00"
                elif not ada_58 and not ada_59:
                    return "Salah satu dari sandi 58 atau 59 harus ada pada jam 00"
            else:
                # pada jam lain keduanya harus tidak ada
                if ada_58 or ada_59:
                    return f"Sandi 58/59 tidak boleh ada pada jam {jam_int:02d}"

            # jika semua valid
            return None

        # Tambahkan kolom hasil validasi
        data['false_sandi58_59'] = data.apply(cek_false_sandi58_59, axis=1)

        # --- 1. Lokasi folder-file
        #folder_coba = '/content/drive/MyDrive/CPNS BMKG Penerbangan/OBP/TugasOBP/Data_Excel/'  # Ganti ke folder tempat file Excel stasiun berada
        file_lokasi = 'Stasiun.xlsx'  # Ganti ke path file lokasi (lon, lat)

        # --- 2. Baca data lokasi stasiun
        df_lokasi = pd.read_excel(file_lokasi, sheet_name="Stasiun")  # pastikan file ini punya kolom: WMO_ID, Nama_stasiun, Longitude, Latitude

        # --- 🔧 Konversi tipe data WMO_ID agar bisa di-merge
        data['station_wmo_id'] = data['station_wmo_id'].astype(str)
        df_lokasi['station_wmo_id'] = df_lokasi['station_wmo_id'].astype(str)

        # --- 5. Gabungkan dengan data lokasi
        df_final = pd.merge(data, df_lokasi, on='station_wmo_id', how='inner')#[['timestamp_data','station_wmo_id', 'NAME','LAT', 'LON','ELEV','sandi_gts', 'Curah_Hujan','Heavy_Rain','Curah_Hujan_Jam','Gale','Kecepatan_angin','Arah_angin','Temperatur','Dew_Point','Tekanan_Permukaan','Tmin','Tmax','Evaporasi','Nh','CL','CM','CH']]
        df_final = df_final.dropna(subset=["LAT", "LON"])

        # --- Pisahkan kolom timestamp menjadi tanggal & jam ---
        df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"], errors="coerce")
        df_final["tanggal"] = df_final["timestamp_data"].dt.date.astype(str)
        df_final["jam"] = df_final["timestamp_data"].dt.strftime("%H:%M")
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # # ⚡ SQLite: Insert ke tabel data_lengkap
        df_to_insert_lengkap = df_final[["tanggal","jam","station_wmo_id",'NAME','LAT',"LON",'ELEV',"REGION_DESC","sandi_gts","nddff","Curah_Hujan","Heavy_Rain","Curah_Hujan_Jam","Gale","Kecepatan_angin","Arah_angin","Temperatur","Tekanan_Permukaan","Tmin","Tmax","Dew_Point"]]
        
        insert_to_sqlite(
            table_name="data_lengkap",
            df=df_to_insert_lengkap,
            key_cols=["tanggal", "jam", "station_wmo_id"]
        )
        
        tanggal_batch_lengkap = df_final["tanggal"].unique().tolist()
        # print(f"✅ Data LENGKAP untuk tanggal {tanggal_batch_lengkap} berhasil diupdate ke SQLite")

#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
        df_final["timestamp_data"] = pd.to_datetime(df_final["timestamp_data"])

        # --- [FIX 1] Hitung 'tanggal_observasi' SEBELUM filter jam 00 ---
        # Ini adalah kunci terpenting: jam 00 dianggap milik hari sebelumnya
        df_final["tanggal_observasi"] = df_final["timestamp_data"].dt.date
        mask_jam00 = df_final["timestamp_data"].dt.hour == 0
        df_final.loc[mask_jam00, "tanggal_observasi"] = (
            df_final.loc[mask_jam00, "tanggal_observasi"] - pd.Timedelta(days=1)
        )
        # Konversi kembali ke object 'date' murni untuk merge yang konsisten
        df_final["tanggal_observasi"] = pd.to_datetime(df_final["tanggal_observasi"]).dt.date

        # --- Ambil data jam 00 (sekarang sudah punya 'tanggal_observasi' yang benar) ---
        df_jam00 = df_final[df_final["timestamp_data"].dt.hour == 0].copy()
        kolom_pilihan = [
            "timestamp_data", "station_wmo_id", "NAME", "sandi_gts", "LAT", "LON", "ELEV",
            "Arah_angin", "Kecepatan_angin", "Gale", "Temperatur", "Tmin", "Tmax",
            "Tekanan_Permukaan", "Curah_Hujan", "Heavy_Rain", "tanggal_observasi" # <-- Penting
        ]
        # Filter hanya kolom yang ada di df_final
        kolom_tersedia = [col for col in kolom_pilihan if col in df_jam00.columns]
        data_00 = df_jam00[kolom_tersedia]

        # --- [FIX 2] Hitung total curah hujan per stasiun DAN per tanggal observasi ---
        df_harian = df_final.groupby(
            ["station_wmo_id", "tanggal_observasi"], as_index=False
        )[["Curah_Hujan_Jam"]].sum()

        # --- [FIX 3] Gabungkan data jam 00 dan akumulasi harian (merge on DUA keys) ---
        data_merge = data_00.merge(
            df_harian, on=["station_wmo_id", "tanggal_observasi"], how="left"
        )

        # --- Pastikan tidak ada nilai NaN & hitung selisih ---
        data_merge["Curah_Hujan"] = data_merge["Curah_Hujan"].fillna(0)
        data_merge["Curah_Hujan_Jam"] = data_merge["Curah_Hujan_Jam"].fillna(0)
        data_merge["selisih"] = (data_merge["Curah_Hujan_Jam"] - data_merge["Curah_Hujan"]).abs()
        data_merge["selisih"] = data_merge["selisih"].round(2)

        # print("--- Hasil Merge dan Perhitungan Selisih (data_merge) ---")
        # print(data_merge[['station_wmo_id', 'tanggal_observasi', 'Curah_Hujan', 'Curah_Hujan_Jam', 'selisih']])
        # print("-" * 40, "\n")

        # --- Pisahkan Data BENAR ---
        toleransi = 2.0 + 1e-6 # Toleransi 2.0, dengan buffer kecil untuk float
        kondisi_benar = (data_merge["selisih"] <= toleransi) | \
                        ((data_merge["Curah_Hujan_Jam"] == 0) & (data_merge["Curah_Hujan"] == 0))
        data_akhir = data_merge[kondisi_benar].copy()

        # --- Pisahkan Data SALAH ---
        data_salah = data_merge.loc[~data_merge.index.isin(data_akhir.index)].copy()

        # print(f"--- Hasil Pemisahan ---")
        # print(f"Data BENAR (masuk data_akhir.db): {len(data_akhir)} stasiun")
        # print(data_akhir[['station_wmo_id', 'tanggal_observasi', 'selisih']])
        # print(f"Data SALAH (masuk data_salah2.db): {len(data_salah)} stasiun")
        # print(data_salah[['station_wmo_id', 'tanggal_observasi', 'selisih']])
        # print("-" * 40, "\n")


#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
 
        # --- Tambahkan kolom tanggal dan jam untuk penyimpanan ---
        data_akhir["timestamp_data"] = pd.to_datetime(data_akhir["timestamp_data"])
        data_akhir["tanggal"] = data_akhir["timestamp_data"].dt.date.astype(str)
        data_akhir["jam"] = data_akhir["timestamp_data"].dt.strftime("%H:%M")

        # Tentukan kolom yang akan disimpan
        cols_akhir = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV","REGION_DESC", "sandi_gts",
            "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
            "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax"
        ]
        # Pastikan hanya menyimpan kolom yang ada
        cols_to_insert_akhir = [col for col in cols_akhir if col in data_akhir.columns]
        df_to_insert_akhir = data_akhir[cols_to_insert_akhir]

        # ⚡ SQLite: Insert ke tabel data_akhir
        insert_to_sqlite(
            table_name="data_akhir",
            df=df_to_insert_akhir,
            key_cols=["tanggal", "jam", "station_wmo_id"]
        )
        
        tanggal_batch_akhir = df_to_insert_akhir["tanggal"].unique().tolist()
        print(f"✅ Data AKHIR untuk tanggal {tanggal_batch_akhir} berhasil disimpan ke SQLite")
        print("-" * 40, "\n")

        # ⚡ SQLite: Insert ke tabel data_gale (hanya data gale dari seluruh hari, tidak hanya jam 00)
        df_gale_source = df_final.copy()
        df_gale_source["timestamp_data"] = pd.to_datetime(df_gale_source["timestamp_data"])
        df_gale_source["tanggal"] = df_gale_source["timestamp_data"].dt.date.astype(str)
        df_gale_source["jam"] = df_gale_source["timestamp_data"].dt.strftime("%H:%M")
        
        # Convert Kecepatan_angin ke numeric dan filter gale events
        df_gale_source['Kecepatan_angin'] = pd.to_numeric(df_gale_source['Kecepatan_angin'], errors='coerce')
        df_gale_filter = df_gale_source[df_gale_source['Kecepatan_angin'] >= 34].copy()
        
        if len(df_gale_filter) > 0:
            # Lookup sandi_gts dari df_final (punya untuk semua jam)
            # Buat lookup dictionary dari df_final
            # Key: (tanggal, station_wmo_id), Value: sandi_gts
            df_final_temp = df_final.copy()
            sandi_gts_lookup = df_final_temp.drop_duplicates(subset=["tanggal", "station_wmo_id"]).set_index(
                ["tanggal", "station_wmo_id"]
            )["sandi_gts"].to_dict()
            
            # Tambahkan sandi_gts ke gale data menggunakan lookup
            df_to_insert_gale = df_gale_filter.copy()
            df_to_insert_gale["sandi_gts"] = df_to_insert_gale.apply(
                lambda row: sandi_gts_lookup.get((row["tanggal"], row["station_wmo_id"]), None),
                axis=1
            )
            
            # Tentukan kolom yang akan disimpan (termasuk sandi_gts sekarang)
            cols_gale = ["tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "REGION_DESC", "sandi_gts", "Kecepatan_angin"]
            cols_gale_available = [col for col in cols_gale if col in df_to_insert_gale.columns]
            df_to_insert_gale = df_to_insert_gale[cols_gale_available]
            
            insert_to_sqlite(
                table_name="data_gale",
                df=df_to_insert_gale,
                key_cols=["tanggal", "jam", "station_wmo_id"]  # Gunakan key_cols untuk menghindari duplikasi
            )
            print(f"[OK] Data GALE berhasil diinsert ke SQLite ({len(df_to_insert_gale)} records)")
        else:
            print(f"[INFO] Tidak ada data GALE (Kecepatan_angin >= 34) untuk tanggal {tanggal_batch_lengkap}")
#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------

        # Cek dulu apakah ada data yang salah
        if not data_salah.empty:
            
            # --- [BAGIAN 1: LOGIKA SCRIPT BARU YANG BENAR] ---
            # Ambil data full jam HANYA untuk stasiun & tanggal observasi yang salah
            # Ini JAUH LEBIH BAIK daripada filter 'Script Lama' Anda
            keys_salah = data_salah[["station_wmo_id", "tanggal_observasi"]].drop_duplicates()
            
            # Merge df_final dengan keys_salah untuk filter multi-kolom
            # Ini akan mengambil jam 03, 06, 09, ... DAN jam 00
            data_salah2 = df_final.merge(
                keys_salah, on=["station_wmo_id", "tanggal_observasi"], how="inner"
            ).copy()

            # --- [BAGIAN 2: FIX UNTUK MASALAH "jam 00 tidak muncul"] ---
            data_salah2["timestamp_data"] = pd.to_datetime(data_salah2["timestamp_data"])
            
            # !!! INI ADALAH KUNCI MASALAH ANDA !!!
            # JANGAN gunakan 'timestamp_data' untuk 'tanggal'
            # data_salah2["tanggal"] = data_salah2["timestamp_data"].dt.date.astype(str) # <-- JANGAN INI
            
            # GUNAKAN 'tanggal_observasi' sebagai 'tanggal'
            # Ini "memaksa" baris jam 00:00 (yang timestamp-nya hari +1)
            # untuk masuk ke 'tanggal' observasi yang sama.
            data_salah2["tanggal"] = data_salah2["tanggal_observasi"].astype(str)
            
            # Kolom 'jam' tetap normal
            data_salah2["jam"] = data_salah2["timestamp_data"].dt.strftime("%H:%M")
            data_salah2["tanggal_observasi"] = data_salah2["tanggal_observasi"].astype(str)
        else:
            # Buat DataFrame kosong jika tidak ada data salah
            data_salah2 = pd.DataFrame(columns=df_final.columns.tolist() + ["tanggal", "jam"])

        # --- Simpan ke MongoDB ---
        cols_salah = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON", "ELEV","REGION_DESC", "sandi_gts",
            "Curah_Hujan", "Heavy_Rain", "Curah_Hujan_Jam", "Gale", "Kecepatan_angin",
            "Arah_angin", "Temperatur", "Tekanan_Permukaan", "Tmin", "Tmax", "tanggal_observasi"
        ]
        cols_to_insert_salah = [col for col in cols_salah if col in data_salah2.columns]
        df_to_insert_salah = data_salah2[cols_to_insert_salah].dropna(subset=['tanggal'])

        # ⚡ SQLite: Insert ke tabel data_suspect
        insert_to_sqlite(
            table_name="data_suspect",
            df=df_to_insert_salah,
            key_cols=["tanggal", "jam", "station_wmo_id"]
        )
        
        tanggal_batch_salah = df_to_insert_salah["tanggal"].unique().tolist()
        if tanggal_batch_salah:
            print(f"✅ Data SALAH untuk tanggal {tanggal_batch_salah} berhasil disimpan ke SQLite")
        else:
            print("ℹ️ Tidak ada data SALAH untuk disimpan.")

#---------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------
        # 0. (opsional) sinkronkan index
        df_final = df_final.reset_index(drop=True)

        # 1. kolom utama yang ingin disimpan (ambil hanya yang ada di df_final)
        kolom_utama = [
            "tanggal", "jam", "station_wmo_id", "NAME", "LAT", "LON",
            "ELEV", "REGION_DESC", "sandi_gts","seksi0", "seksi1","seksi3",
            "iihvv","yy","gg","sandi6","sandi63"
        ]
        kolom_utama = [c for c in kolom_utama if c in df_final.columns]

        # 2. ambil daftar kolom false_ dari df_final (pastikan hanya yang ada)
        #    <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
        kolom_false = [c for c in df_final.columns if c.startswith("false_")]

        # 3. buat mask: True kalau kolom false_ memiliki isi non-empty (bukan NaN dan bukan "")
        #    logika: untuk setiap kolom false_ -> (notna) AND (strip() != "")
        if len(kolom_false) == 0:
            print("⚠️ Tidak ditemukan kolom yang diawali 'false_' di dataframe `df_final`.")
            # <-- PERUBAHAN: Menggunakan panjang df_final
            mask_error = pd.Series([False]*len(df_final), index=df_final.index)
        else:
            mask_per_kol = []
            for c in kolom_false:
                # <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
                s = df_final[c]
                # convert to string for strip but keep check notna()
                has_value = s.notna() & s.astype(str).str.strip().ne("")
                mask_per_kol.append(has_value)
            # gabungkan: baris error jika ada minimal satu kolom false_ yang berisi
            mask_error = pd.concat(mask_per_kol, axis=1).any(axis=1)

        # 4. kolom akhir yang akan disimpan: gabungkan kolom_utama + kolom_false
        #    (Sekarang lebih sederhana karena semua kolom pasti ada di df_final)
        kolom_simpan = list(kolom_utama)  # salin
        for c in kolom_false:
            if c not in kolom_simpan:
                kolom_simpan.append(c)

        # 5. slice df_final berdasarkan mask_error
        data_error = df_final.loc[mask_error, kolom_simpan].copy()

        # 6. buat kolom Daftar_Kesalahan (gabungan isi kolom_false dari df_final)
        if len(data_error) > 0 and len(kolom_false) > 0:
            # <-- PERUBAHAN: Mengambil dari df_final, bukan 'data'
            #    Kita gunakan df_final.loc[mask_error] untuk mengambil baris yang sama
            #    yang baru saja kita masukkan ke data_error.
            daftar = df_final.loc[mask_error, kolom_false].apply(
                lambda row: "; ".join([str(v).strip() for v in row if pd.notna(v) and str(v).strip() != ""]),
                axis=1
            ).reset_index(drop=True)
            
            # reset index supaya align dengan data_error
            data_error = data_error.reset_index(drop=True)
            data_error["Daftar_Kesalahan"] = daftar
        else:
            # Pastikan kolom ada meskipun tidak ada error, untuk konsistensi skema
            data_error["Daftar_Kesalahan"] = "" 

       # 7. simpan ke SQLite (strategi: hapus data lama yg cocok, lalu append data baru)
        # if len(data_error) > 0:
        #     # ⚡ SQLite: Insert ke tabel data_error
        #     insert_to_sqlite(
        #         table_name="data_error",
        #         df=data_error,
        #         key_cols=["tanggal", "jam", "station_wmo_id"]
        #     )
        # else:
        #     print("🎉 Tidak ada baris error — tidak ada yang disimpan.")


    except Exception as e:
        print(f"❌ GAGAL TOTAL memproses tanggal {TANGGAL}. Error: {e}")
        continue # Lanjut ke hari berikutnya jika terjadi error

print(f"\n{'='*30}")
print(f"✅ SELURUH PROSES UNTUK RANGE {TANGGAL_AWAL} s/d {TANGGAL_AKHIR} TELAH SELESAI.")
print(f"{'='*30}")
print(f"📊 Total hari yang diproses: {len(list_tanggal)}")
print(f"📍 Data tersimpan di SQLite - Database: {DB_PATH}")
print(f"📂 Tabel: data_lengkap, data_akhir, data_suspect, data_error")
print(f"{'='*30}\n")

# Tutup koneksi SQLite
conn.close()
