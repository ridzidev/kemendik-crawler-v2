import sqlite3

DB_NAME = "sekolah.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Satu tabel untuk semua informasi (30+ Kolom)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sekolah (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        -- FASE 1
        npsn TEXT UNIQUE,
        nama TEXT,
        status TEXT,
        bentuk_pendidikan TEXT,
        alamat TEXT,
        desa TEXT,
        kecamatan TEXT,
        kabupaten TEXT,
        provinsi TEXT,
        
        -- FASE 2 (Identitas & Perijinan)
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
        
        -- FASE 2 (Sarpras)
        luas_tanah TEXT,
        internet_1 TEXT,
        internet_2 TEXT,
        sumber_listrik TEXT,
        
        -- FASE 2 (Kontak)
        fax TEXT,
        telepon TEXT,
        email TEXT,
        website TEXT,
        operator TEXT,
        
        -- FASE 2 (Peta)
        lintang TEXT,
        bujur TEXT,
        
        fase2_done INTEGER DEFAULT 0
    )""")

    # Tabel Progress
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS progress (
        id INTEGER PRIMARY KEY,
        prov INTEGER DEFAULT 1,
        kab INTEGER DEFAULT 1,
        kec INTEGER DEFAULT 1,
        is_running INTEGER DEFAULT 0,
        fase2_running INTEGER DEFAULT 0
    )""")

    cursor.execute("INSERT OR IGNORE INTO progress (id, prov, kab, kec, is_running, fase2_running) VALUES (1, 1, 1, 1, 0, 0)")
    conn.commit()
    conn.close()
    print("✅ Database Siap.")