import requests
import time
import re
from bs4 import BeautifulSoup
from db import get_connection
from concurrent.futures import ThreadPoolExecutor

class KemendikCrawler:
    def __init__(self, max_workers=8):
        self.base_url = "https://referensi.data.kemendikdasmen.go.id"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.max_workers = max_workers
        self.is_active = False
        self.fase2_active = False
        self.live_kode = "000000"
        self.live_npsn = "IDLE"

    # --- FASE 1: BRUTE FORCE ---
    def fetch_kecamatan(self, p, k, kc):
        if not self.is_active: return
        kode6 = f"{p:02}{k:02}{kc:02}"
        self.live_kode = kode6
        
        try:
            url = f"{self.base_url}/pendidikan/dikdas/{kode6}/3"
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200 or "Data tidak ditemukan" in resp.text: return
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Parsing Breadcrumb (Nama Asli)
            prov_n, kab_n, kec_n = f"PROV_{p:02}", f"KAB_{k:02}", f"KEC_{kc:02}"
            bc = soup.find(lambda tag: tag.name == "div" and "Indonesia" in tag.text)
            if bc:
                txt = bc.get_text(strip=True)
                parts = [p.strip() for p in re.split(r'>>|»', txt)]
                if len(parts) > 1: prov_n = parts[1]
                if len(parts) > 2: kab_n = parts[2]
                if len(parts) > 3: kec_n = parts[3].split("DAFTAR")[0].strip()

            table = soup.find("table", {"id": "table1"})
            if not table: return
            rows = table.find("tbody").find_all("tr")
            
            batch = []
            for row in rows:
                c = row.find_all("td")
                if len(c) < 6: continue
                batch.append((
                    c[1].text.strip(), c[2].text.strip(), c[5].text.strip(),
                    "DIKDAS", c[3].text.strip(), c[4].text.strip(), kec_n, kab_n, prov_n
                ))
            
            if batch:
                with get_connection() as conn:
                    conn.executemany("""INSERT OR IGNORE INTO sekolah 
                    (npsn, nama, status, bentuk_pendidikan, alamat, desa, kecamatan, kabupaten, provinsi) 
                    VALUES (?,?,?,?,?,?,?,?,?)""", batch)
                    conn.execute("UPDATE progress SET prov=?, kab=?, kec=? WHERE id=1", (p, k, kc))
                    conn.commit()
        except: pass

    def run_f1(self):
        self.is_active = True
        with get_connection() as conn:
            prog = conn.execute("SELECT * FROM progress WHERE id=1").fetchone()
        
        for p in range(prog['prov'], 40):
            if not self.is_active: break
            for k in range(prog['kab'] if p == prog['prov'] else 1, 100):
                if not self.is_active: break
                for kc in range(prog['kec'] if (p == prog['prov'] and k == prog['kab']) else 1, 100):
                    if not self.is_active: break
                    self.fetch_kecamatan(p, k, kc)
                    time.sleep(0.05)
        self.is_active = False

    # --- FASE 2: FULL ENRICHMENT (SEMUA KOLOM) ---
    def fetch_detail(self, npsn):
        if not self.fase2_active: return
        self.live_npsn = npsn
        try:
            url = f"{self.base_url}/tabs.php?npsn={npsn}"
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200: return
            soup = BeautifulSoup(resp.text, "html.parser")

            def g(label):
                # Cari TD yang teksnya pas banget sama label, lalu ambil TD sebelahnya
                target = soup.find("td", string=re.compile(f"^{label}$", re.I))
                if target:
                    val_td = target.find_next_sibling("td")
                    if val_td and val_td.text.strip() == ":":
                        val_td = val_td.find_next_sibling("td")
                    return val_td.get_text(strip=True) if val_td else ""
                return ""

            # Logika khusus Internet
            i1, i2 = "", ""
            itd = soup.find("td", string=re.compile("Akses Internet", re.I))
            if itd:
                i1 = itd.find_next_sibling("td").find_next_sibling("td").text.strip()
                row2 = itd.find_parent("tr").find_next_sibling("tr")
                if row2: i2 = row2.find_all("td")[-1].text.strip()

            # Mapping Data
            data = {
                'link': soup.select_one("a[href*='profil-sekolah']")['href'] if soup.select_one("a[href*='profil-sekolah']") else "",
                'bp_d': g("Bentuk Pendidikan"),
                'pembina': g("Kementerian Pembina"),
                'naungan': g("Naungan"),
                'npyp': g("NPYP"),
                'sk_p': g("No. SK. Pendirian"),
                'tgl_p': g("Tanggal SK. Pendirian"),
                'sk_o': g("Nomor SK Operasional"),
                'tgl_o': g("Tanggal SK Operasional"),
                'sk_l': soup.find("a", string=re.compile("Lihat SK Operasional", re.I))['href'] if soup.find("a", string=re.compile("Lihat SK Operasional", re.I)) else "",
                'tgl_u': g("Tanggal Upload SK Op."),
                'akr': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)).text.strip() if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'akr_l': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I))['href'] if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'luas': g("Luas Tanah"),
                'listrik': g("Sumber Listrik"),
                'fax': g("Fax"),
                'telp': g("Telepon"),
                'mail': g("Email"),
                'web': g("Website"),
                'ops': g("Operator"),
                'lat': re.search(r"Lintang:\s*([\-\d\.]+)", resp.text).group(1) if re.search(r"Lintang:\s*([\-\d\.]+)", resp.text) else "",
                'lng': re.search(r"Bujur:\s*([\-\d\.]+)", resp.text).group(1) if re.search(r"Bujur:\s*([\-\d\.]+)", resp.text) else "",
                'pr_r': g("Propinsi/Luar Negeri \(LN\)"),
                'kb_r': g("Kab.-Kota/Negara \(LN\)"),
                'kc_r': g("Kecamatan/Kota \(LN\)")
            }

            with get_connection() as conn:
                conn.execute("""UPDATE sekolah SET 
                    npsn_link=?, bentuk_pendidikan_detail=?, kementerian_pembina=?, naungan=?, npyp=?, 
                    sk_pendirian=?, tgl_sk_pendirian=?, sk_operasional=?, tgl_sk_operasional=?, 
                    sk_operasional_link=?, tgl_upload_sk=?, akreditasi=?, akreditasi_link=?, 
                    luas_tanah=?, internet_1=?, internet_2=?, sumber_listrik=?, fax=?, telepon=?, 
                    email=?, website=?, operator=?, lintang=?, bujur=?, 
                    provinsi=COALESCE(NULLIF(?,''),provinsi), kabupaten=COALESCE(NULLIF(?,''),kabupaten), 
                    kecamatan=COALESCE(NULLIF(?,''),kecamatan), fase2_done=1 WHERE npsn=?""", 
                    (data['link'], data['bp_d'], data['pembina'], data['naungan'], data['npyp'], data['sk_p'], data['tgl_p'],
                     data['sk_o'], data['tgl_o'], data['sk_l'], data['tgl_u'], data['akr'], data['akr_l'],
                     data['luas'], i1, i2, data['listrik'], data['fax'], data['telp'], data['mail'], data['web'], 
                     data['ops'], data['lat'], data['lng'], data['pr_r'], data['kb_r'], data['kc_r'], npsn))
                conn.commit()
        except: pass

    def run_f2(self):
        self.fase2_active = True
        while self.fase2_active:
            with get_connection() as conn:
                todo = conn.execute("SELECT npsn FROM sekolah WHERE fase2_done=0 LIMIT 20").fetchall()
            if not todo:
                self.fase2_active = False
                break
            with ThreadPoolExecutor(max_workers=10) as ex:
                for r in todo:
                    if not self.fase2_active: return
                    ex.submit(self.fetch_detail, r['npsn'])
                    time.sleep(0.1)

crawler_instance = KemendikCrawler()