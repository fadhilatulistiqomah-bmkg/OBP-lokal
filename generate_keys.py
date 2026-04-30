from pathlib import Path
import pickle
#import bcrypt
import streamlit_authenticator as stauth    

names =["sobp"]
usernames = ["sobp"]
passwords = ["XXX"]

hashed_passwords = stauth.Hasher(passwords).generate()

file_path = Path(__file__).parent / "hashed_pw.pkl"
with file_path.open("wb") as file:
    pickle.dump(hashed_passwords, file) 
# # --- Sesuaikan user / password di sini ---
# users = [
#     {"username": "sobp", "name": "Sobp", "password": "XXX"},
#     # tambah user lain kalau perlu:
#     # {"username": "user2", "name": "Nama 2", "password": "pass2"},
# ]

# # --- Hash semua password dengan bcrypt ---
# for u in users:
#     pw_bytes = u["password"].encode("utf-8")
#     hashed = bcrypt.hashpw(pw_bytes, bcrypt.gensalt())
#     # simpan sebagai string (decode)
#     u["password"] = hashed.decode("utf-8")

# # --- Susun struktur credentials sesuai streamlit-authenticator ---
# # streamlit-authenticator mengharapkan dict: {"usernames": {username: {"name":..., "password": ...}, ...}}
# credentials = {"usernames": {}}
# for u in users:
#     credentials["usernames"][u["username"]] = {
#         "name": u["name"],
#         "password": u["password"]
#     }

# # --- Simpan ke file pickle ---
# file_path = Path(__file__).parent / "hashed_pw.pkl"
# with file_path.open("wb") as f:
#     pickle.dump(credentials, f)

# print(f"Saved hashed credentials to: {file_path}")
