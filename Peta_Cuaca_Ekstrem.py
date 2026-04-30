import streamlit as st
import folium
from folium import plugins
from streamlit_folium import st_folium
import pandas as pd
from folium import CustomIcon
import sqlite3
from datetime import date, timedelta
import re
import pickle
from pathlib import Path
import streamlit_authenticator as stauth
import time # Tambahkan ini

# ⚡ SQLite Connection Setup
DB_PATH = "cuaca_ekstrem.db"

# Helper function untuk query data dari SQLite
def get_data_from_sqlite(table_name, where_clause=None, params=None):
    """
    Ambil data dari SQLite table
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        if where_clause:
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
            df = pd.read_sql_query(query, conn, params=params if params else ())
        else:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error mengambil data dari {table_name}: {e}")
        return pd.DataFrame()

# --- Konfigurasi halaman (hanya boleh dipanggil sekali di paling atas) ---
st.set_page_config(page_title="Peta Cuaca Ekstrem", layout="wide")
names = ["sobp"]
usernames = ["sobp"]

file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("rb") as file:
    hashed_passwords = pickle.load(file)

authenticator = stauth.Authenticate(names, usernames, hashed_passwords,"usernames","abcdef", cookie_expiry_days=30)

# --- Proses Login ---
names, authentication_status, usernames = authenticator.login("Login", "main")

# --- Penanganan Status Autentikasi ---
if authentication_status == False:
    st.error("Username/password salah")
    st.stop() # Hentikan eksekusi setelah menampilkan error
if authentication_status == None:
    st.warning("Silakan masukkan username dan password")
    st.stop() # Hentikan eksekusi jika belum login/mencoba login

# --- KONTEN APLIKASI TERPROTEKSI DIMULAI DI SINI ---
if authentication_status: 
    # Tambahkan Tombol Logout di sidebar atau di header
    st.sidebar.title(f"Selamat Datang, {usernames}!")
    authenticator.logout("Logout", "sidebar")
    
    # Impor utility hanya jika sudah login
    from utils.ui import setup_header,setup_sidebar_footer
    setup_header()
    setup_sidebar_footer()

    # ==========================================================
    # 💾 2️⃣ PENGAMBILAN DATA
    # ==========================================================

    # --- SQLite Tables ---
    table_akhir = "data_akhir"
    table_lengkap = "data_lengkap"

    # --- Widget utama untuk memilih tanggal ---
    pilih_tanggal = st.date_input(
        "📅 Pilih tanggal:",
        value=date.today(),
        min_value=date(2026, 1, 1),
        max_value=date(2026, 12, 31)
    )

    # --- 2a. Ambil data STASIUN & HEAVY RAIN dari SQLite table `data_akhir` ---
    tanggal_filter = pilih_tanggal.strftime("%Y-%m-%d")
    df_main = get_data_from_sqlite(table_akhir, "tanggal = ?", (tanggal_filter,))

    # Sort data
    if not df_main.empty:
        df_main = df_main.sort_values(by=['station_wmo_id', 'jam']).reset_index(drop=True)

    tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)

    # --- 2b. Ambil data GALE dari SQLite table `data_lengkap` ---
    # Query untuk data jam 01-23 UTC hari sebelumnya DAN jam 00 UTC hari yang dipilih
    tanggal_sebelumnya_str = tanggal_sebelumnya.strftime("%Y-%m-%d")
    tanggal_str = pilih_tanggal.strftime("%Y-%m-%d")
    
    conn = sqlite3.connect(DB_PATH)
    query_gale = """
    SELECT * FROM """ + table_lengkap + """ WHERE 
    (tanggal = ? AND jam != '00:00' AND Kecepatan_angin >= 34) OR 
    (tanggal = ? AND jam = '00:00' AND Kecepatan_angin >= 34)
    """
    df_gale = pd.read_sql_query(query_gale, conn, params=(tanggal_sebelumnya_str, tanggal_str))
    conn.close()

    # Sort data
    if not df_gale.empty:
        df_gale = df_gale.sort_values(by=['tanggal', 'jam', 'station_wmo_id']).reset_index(drop=True)

    # Pastikan tipe data numeric untuk filtering
    if not df_gale.empty:
        df_gale['Kecepatan_angin'] = pd.to_numeric(df_gale['Kecepatan_angin'], errors='coerce').fillna(0)


    # ==========================================================
    # 🗺️ 3️⃣ PEMBUATAN PETA
    # ==========================================================

    m = folium.Map(location=[-2, 118], zoom_start=5, tiles=None)

    # Tambahkan tile CartoDB Voyager tanpa label attribution
    m = folium.Map(location=[-2, 118], zoom_start=5, tiles=None)

    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png",
        attr='© OpenStreetMap | © CARTO',
        name='Base Map',
        control=False
    ).add_to(m)
    # Buat LayerGroup untuk event cuaca ekstrem
    gale_layer = folium.FeatureGroup(name="Gale", show=True)
    hr_layer   = folium.FeatureGroup(name="Heavy Rain", show=True)

    # --- Loop 1: Plot semua stasiun dasar dan ikon Heavy Rain dari `df_main` ---
    if not df_main.empty:
        # Buat satu set untuk melacak stasiun yang sudah diplot agar tidak duplikat
        plotted_stations = set()
        for _, row in df_main.iterrows():
            lat, lon, station_id = row["LAT"], row["LON"], row["station_wmo_id"]
            wind_text = "Calm" if pd.isna(row['Kecepatan_angin']) else f"{row['Kecepatan_angin']} knot"
            rain_text = "-" if pd.isna(row['Curah_Hujan']) else f"{row['Curah_Hujan']} mm/hari"
            # Hanya plot titik biru stasiun SEKALI saja
            if pd.notna(lat) and pd.notna(lon) and station_id not in plotted_stations:

                folium.CircleMarker(
                    location=[lat, lon],
                    radius=2,
                    color="blue",
                    fill=True,
                    fill_color="blue",
                    fill_opacity=0.7,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>"
                        f"ID Stasiun: {row['station_wmo_id']} <br>"
                        f"Temperatur: {row['Temperatur']} °C<br>"
                        f"Curah Hujan: {rain_text} <br>"
                        f"Kecepatan Angin: {wind_text}"
                        f"</div>"
                    ),
                    tooltip=row["NAME"]
                ).add_to(m)
                plotted_stations.add(station_id)

            # Cek dan plot ikon Heavy Rain untuk setiap kejadian
            try:
                hr_value = float(row["Heavy_Rain"])
                if hr_value > 0:
                    # Ganti 'cloud_rain.png' dengan URL gambar yang valid atau tangani sebagai aset
                    # Untuk contoh ini, saya asumsikan Anda memiliki file 'cloud_rain.png' yang dapat diakses
                    rain_icon = CustomIcon("cloud_rain.png", icon_size=(30, 30)) 
                    folium.Marker(
                        location=[lat, lon], icon=rain_icon,
                        popup=(
                            f"<div style='font-size:10px;width:200px;'>"
                            f"<b>{row['NAME']}</b><br>{station_id}<br>🌧️ Heavy Rain: {hr_value} mm/hari"
                            f"</div>"
                        )
                    ).add_to(hr_layer)
            except (ValueError, TypeError):
                pass

    # --- Loop 2: Plot ikon Gale dari `df_gale` ---
    if not df_gale.empty:
        for _, row in df_gale.iterrows():
            lat, lon = row["LAT"], row["LON"]
            if pd.notna(lat) and pd.notna(lon):
                # Ganti 'wind.png' dengan URL gambar yang valid atau tangani sebagai aset
                wind_icon = CustomIcon("wind.png", icon_size=(30, 30))
                folium.Marker(
                    location=[lat, lon], icon=wind_icon,
                    popup=(
                        f"<div style='font-size:10px;width:200px;'>"
                        f"<b>{row['NAME']}</b><br>{row['station_wmo_id']}<br> Gale: {row['Kecepatan_angin']} knot <br> Pukul : {row['jam']} UTC"
                        f"</div>"
                    )
                ).add_to(gale_layer)

    # Tambahkan layer ke peta
    gale_layer.add_to(m)
    hr_layer.add_to(m)
    folium.LayerControl().add_to(m)

    # --- TAMBAHKAN BLOK KODE INI ---
    plugins.Fullscreen(
        position="topright",
        title="Lihat Fullscreen",
        title_cancel="Keluar Fullscreen",
        force_separate_button=True,
    ).add_to(m)

    st_folium(m, width=None, height=500)


    # ==========================================================
    # 📋 4️⃣ HIGHLIGHT & TABEL DATA
    # ==========================================================
    tanggal_sebelumnya = pilih_tanggal - timedelta(days=1)
    st.markdown(f"""
    <hr style="border: 1px solid #ccc; margin: 20px 0;">
    <div style='background-color:#e9f2fb; padding:15px; border-radius:10px;'>
    <b>📌 Highlight:</b><br>
    1. Data yang ditampilkan merupakan data dari 00.01–23.59 UTC (<b>{tanggal_sebelumnya.strftime("%d %B %Y")}</b>)<br>
    2. Threshold untuk Heavy Rain adalah <b>50 mm/hari</b><br>
    3. Threshold untuk Gale adalah <b>34 knot</b>
    </div>
    """, unsafe_allow_html=True)


    # --- Tabel 1: Heavy Rain (dari df_main) ---
    st.markdown("### Daftar Stasiun dengan Kejadian Heavy Rain")
    df_hr = df_main.copy()
    df_hr["Heavy_Rain"] = pd.to_numeric(df_hr["Heavy_Rain"], errors="coerce")
    df_hr_filtered = df_hr[df_hr["Heavy_Rain"] > 0]
    if not df_hr_filtered.empty:
        df_tabel_hr = df_hr_filtered[[
            "station_wmo_id", "NAME","sandi_gts" ,"Heavy_Rain"
        ]].sort_values(by=["Heavy_Rain"], ascending=False).reset_index(drop=True)
        df_tabel_hr.index = df_tabel_hr.index + 1
        df_tabel_hr_display = df_tabel_hr.copy()
        regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
        
        df_tabel_hr_display['sandi_gts'] = (
            df_tabel_hr_display['sandi_gts']
            .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0] 
            .fillna("") 
            .str.replace(r'CCA', '', regex=True) 
            .str.replace(r'CCB', '', regex=True) 
            .str.replace(r'\s+', ' ', regex=True) 
            .str.strip()
        )

        st.dataframe(
            df_tabel_hr_display,
            use_container_width=True,
            column_config={
                "station_wmo_id": st.column_config.Column("WMO ID Stasiun"),
                "NAME": st.column_config.Column("Nama Stasiun"),
                "sandi_gts": st.column_config.Column("Sandi GTS"),
                "Heavy_Rain": st.column_config.Column("Curah Hujan (mm/hari)")
            }
        )

        st.info(f"Total kejadian Heavy Rain (curah hujan ≥ 50 mm/hari) terdeteksi: {len(df_hr_filtered)} stasiun.")
    else:
        st.info("Tidak ada stasiun dengan kejadian Heavy Rain pada tanggal yang dipilih.")

    # --- Tabel 2: Gale (dari df_gale) ---
    st.markdown("### Daftar Stasiun dengan Kejadian Gale")

    if not df_gale.empty:
        df_tabel_gale = df_gale[['station_wmo_id', 'NAME', 'jam', 'sandi_gts', 'Kecepatan_angin']].rename(columns={
            'station_wmo_id': 'WMO ID Stasiun', 'NAME': 'Nama Stasiun', 'jam': 'Jam Observasi (UTC)',
            'sandi_gts': 'Sandi GTS', 'Kecepatan_angin': 'Kecepatan Angin (knot)'
        }).sort_values(by="Kecepatan Angin (knot)", ascending=False).reset_index(drop=True)
        
        df_tabel_gale.index = df_tabel_gale.index + 1
        
        df_tabel_gale_display = df_tabel_gale.copy()

        regex_ekstraksi_kondisional = r'(\b(AAXX|AXX|AAAXX|AAX|AXXX)\b.*?(?:=|$))'
        
        df_tabel_gale_display['Sandi GTS'] = (
            df_tabel_gale_display['Sandi GTS']
            .str.extract(regex_ekstraksi_kondisional, flags=re.DOTALL)[0]
            .fillna("") 
            .str.replace(r'CCA', '', regex=True)
            .str.replace(r'CCB', '', regex=True)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip() 
        )

        st.dataframe(df_tabel_gale_display, use_container_width=True)
        
        st.info(f"Total kejadian Gale (kecepatan angin ≥ 34 knot) terdeteksi: {len(df_gale)} stasiun.")
    else:
        st.info("Tidak ada stasiun dengan kejadian Gale pada tanggal yang dipilih.")