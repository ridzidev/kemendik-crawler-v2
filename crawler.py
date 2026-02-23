import requests
import time
import re
from bs4 import BeautifulSoup
from db import get_connection
from concurrent.futures import ThreadPoolExecutor, as_completed

class KemendikCrawler:
    def __init__(self, max_workers=15):
        self.base_url = "https://referensi.data.kemendikdasmen.go.id"
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers)
        self.session.mount('https://', adapter)
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        self.max_workers = max_workers
        self.is_active = False
        self.fase2_active = False
        self.live_kode = "000000"
        self.live_npsn = "Pending"

    # --- HELPER UPDATE PROGRESS KE DB ---
    def update_db_progress(self, p, k, kc):
        try:
            with get_connection() as conn:
                conn.execute("UPDATE progress SET prov=?, kab=?, kec=? WHERE id=1", (p, k, kc))
                conn.commit()
        except: pass

    # --- FASE 1: WORKER ---
    def fetch_kecamatan(self, p, k, kc):
        if not self.is_active: return
        kode6 = f"{p:02}{k:02}{kc:02}"
        self.live_kode = kode6
        
        # url = f"{self.base_url}/pendidikan/dikdas/{kode6}/3"
        url = f"{self.base_url}/pendidikan/dikmen/{kode6}/3"
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code != 200 or "Data tidak ditemukan" in resp.text:
                self.update_db_progress(p, k, kc)
                return
            
            soup = BeautifulSoup(resp.text, "html.parser")
            prov_n, kab_n, kec_n = f"PROV_{p:02}", f"KAB_{k:02}", f"KEC_{kc:02}"
            
            # Parsing Breadcrumb (Anti-Trim)
            bc_el = soup.find(lambda tag: tag.name == "div" and "Indonesia" in tag.text)
            if bc_el:
                parts = [x.strip() for x in re.split(r'>>|»', bc_el.get_text(strip=True))]
                if len(parts) > 1: prov_n = parts[1]
                if len(parts) > 2: kab_n = parts[2]
                if len(parts) > 3: kec_n = parts[3].split("DAFTAR")[0].strip()

            table = soup.find("table", {"id": "table1"})
            if not table: 
                self.update_db_progress(p, k, kc)
                return
            
            rows = table.find("tbody").find_all("tr")
            batch = []
            for row in rows:
                c = row.find_all("td")
                if len(c) < 6: continue
                # batch.append((
                #     c[1].text.strip(), c[2].text.strip(), c[5].text.strip(),
                #     "DIKDAS", c[3].text.strip(), c[4].text.strip(), kec_n, kab_n, prov_n
                # ))
                batch.append((
                    c[1].text.strip(), c[2].text.strip(), c[5].text.strip(),
                    "DIKMEN", c[3].text.strip(), c[4].text.strip(), kec_n, kab_n, prov_n
                ))
            
            if batch:
                with get_connection() as conn:
                    conn.executemany("""INSERT OR IGNORE INTO sekolah 
                    (npsn, nama, status, bentuk_pendidikan, alamat, desa, kecamatan, kabupaten, provinsi) 
                    VALUES (?,?,?,?,?,?,?,?,?)""", batch)
                    conn.commit()
            
            self.update_db_progress(p, k, kc)
        except:
            self.update_db_progress(p, k, kc)

    def run_f1(self):
        self.is_active = True
        with get_connection() as conn:
            prg = conn.execute("SELECT * FROM progress WHERE id=1").fetchone()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for p in range(prg['prov'], 40):
                if not self.is_active: break
                for k in range(prg['kab'] if p == prg['prov'] else 1, 100):
                    if not self.is_active: break
                    
                    futures = []
                    start_kc = prg['kec'] if (p == prg['prov'] and k == prg['kab']) else 1
                    for kc in range(start_kc, 100):
                        if not self.is_active: break
                        futures.append(executor.submit(self.fetch_kecamatan, p, k, kc))
                    
                    # Cek hasil dan berikan kesempatan untuk Pause
                    for future in as_completed(futures):
                        if not self.is_active:
                            executor.shutdown(wait=False, cancel_futures=True)
                            return
            
        self.is_active = False

    # --- FASE 2: FULL DATA ENRICHMENT ---
    def fetch_detail(self, npsn):
        if not self.fase2_active: return
        self.live_npsn = npsn
        url = f"{self.base_url}/tabs.php?npsn={npsn}"
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code != 200: return
            soup = BeautifulSoup(resp.text, "html.parser")

            def g(label):
                target = soup.find("td", string=re.compile(f"^{label}$", re.I))
                if target:
                    val_td = target.find_next_sibling("td")
                    if val_td and val_td.text.strip() == ":":
                        val_td = val_td.find_next_sibling("td")
                    return val_td.get_text(strip=True) if val_td else ""
                return ""

            # Internet
            i1, i2 = "", ""
            itd = soup.find("td", string=re.compile("Akses Internet", re.I))
            if itd:
                i1 = itd.find_next_sibling("td").find_next_sibling("td").text.strip()
                row2 = itd.find_parent("tr").find_next_sibling("tr")
                if row2:
                    c2 = row2.find_all("td")
                    if len(c2) > 0: i2 = c2[-1].text.strip()

            # Mapping SEMUA Kolom
            d = {
                'nlink': soup.select_one("a[href*='profil-sekolah']")['href'] if soup.select_one("a[href*='profil-sekolah']") else "",
                'bpd': g("Bentuk Pendidikan"),
                'pemb': g("Kementerian Pembina"),
                'naung': g("Naungan"),
                'npyp': g("NPYP"),
                'skp': g("No. SK. Pendirian"),
                'tglp': g("Tanggal SK. Pendirian"),
                'sko': g("Nomor SK Operasional"),
                'tglo': g("Tanggal SK Operasional"),
                'skol': soup.find("a", string=re.compile("Lihat SK Operasional", re.I))['href'] if soup.find("a", string=re.compile("Lihat SK Operasional", re.I)) else "",
                'tglu': g("Tanggal Upload SK Op."),
                'akr': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)).text.strip() if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'akrl': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I))['href'] if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'luas': g("Luas Tanah"),
                'list': g("Sumber Listrik"),
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
                    (d['nlink'], d['bpd'], d['pemb'], d['naung'], d['npyp'], d['skp'], d['tglp'], d['sko'], d['tglo'], d['skol'], d['tglu'], d['akr'], d['akrl'],
                     d['luas'], i1, i2, d['list'], d['fax'], d['telp'], d['mail'], d['web'], d['ops'], d['lat'], d['lng'], d['pr_r'], d['kb_r'], d['kc_r'], npsn))
                conn.commit()
        except: pass

    def run_f2(self):
        self.fase2_active = True
        while self.fase2_active:
            with get_connection() as conn:
                todo = conn.execute("SELECT npsn FROM sekolah WHERE fase2_done=0 LIMIT 100").fetchall()
            if not todo: break
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = [ex.submit(self.fetch_detail, r['npsn']) for r in todo]
                for future in as_completed(futures):
                    if not self.fase2_active:
                        ex.shutdown(wait=False, cancel_futures=True)
                        return
        self.fase2_active = False

crawler_instance = KemendikCrawler()