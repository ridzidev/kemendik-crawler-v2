import sqlite3

DB_NAME = "sekolah.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # Mode Kebut WAL (Write-Ahead Logging)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. Tabel Sekolah (DATA FINAL)
    # Gue tambahin constraint UNIQUE biar gak duplikat NPSN
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sekolah (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        npsn TEXT UNIQUE,
        nama TEXT,
        status TEXT,
        bentuk_pendidikan TEXT,
        alamat TEXT,
        desa TEXT,
        kecamatan TEXT,
        kabupaten TEXT,
        provinsi TEXT,
        
        -- Detail Fase 2
        npsn_link TEXT,
        bentuk_pendidikan_detail TEXT,
        kementerian_pembina TEXT,
        naungan TEXT,
        npyp TEXT,
        sk_pendirian TEXT,
        tgl_sk_pendirian TEXT,
        sk_operasional TEXT,
        tgl_sk_operasional TEXT,
        sk_operasional_link TEXT,
        tgl_upload_sk TEXT,
        akreditasi TEXT,
        akreditasi_link TEXT,
        luas_tanah TEXT,
        internet_1 TEXT,
        internet_2 TEXT,
        sumber_listrik TEXT,
        fax TEXT,
        telepon TEXT,
        email TEXT,
        website TEXT,
        operator TEXT,
        lintang TEXT,
        bujur TEXT,
        fase2_done INTEGER DEFAULT 0
    )""")

    # 2. Tabel Antrian Wilayah (PENGGANTI PROGRESS LAMA)
    # Ini otak dari algoritma baru lo.
    # kode: misal '010000', '010100', '010101'
    # level: 1 (Prov), 2 (Kab), 3 (Kec - Siap Panen)
    # status: 0 (Belum discan), 1 (Sudah discan/selesai)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wilayah_queue (
        kode TEXT PRIMARY KEY,
        nama TEXT,
        level INTEGER,
        parent_kode TEXT,
        status INTEGER DEFAULT 0
    )""")

    conn.commit()
    conn.close()
    print("✅ DB Fresh & Ready for Hierarchy Crawling.")

if __name__ == "__main__":
    init_db()