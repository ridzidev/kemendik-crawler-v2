import requests
import re
import time
import urllib3
from bs4 import BeautifulSoup
from db import get_connection
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Matikan warning SSL dari urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class KemendikCrawler:
    def __init__(self, max_workers=5):
        self.base_url = "https://referensi.data.kemendikdasmen.go.id/pendidikan/dikmen"
        self.root_url = "https://referensi.data.kemendikdasmen.go.id/pendidikan/dikmen"
        
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(pool_connections=max_workers, pool_maxsize=max_workers, max_retries=retries)
        self.session.mount('https://', adapter)
        
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        })
        self.session.verify = False # Bypass SSL
        
        self.is_active = False
        self.fase2_active = False
        self.live_kode = "IDLE"
        self.live_npsn = "-"

    def _get_soup(self, url):
        try:
            resp = self.session.get(url, timeout=20)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            pass
        return None

    # --- PARSER FASE 1 ---
    def _parse_region_table(self, soup, child_level):
        results = []
        if not soup: return results
        table = soup.find("table", {"id": "table1"})
        if not table: return results

        for row in table.find("tbody").find_all("tr"):
            link_td = row.find("td", {"class": "link1"})
            if link_td and link_td.find("a"):
                a_tag = link_td.find("a")
                href = a_tag.get('href', '')
                nama = a_tag.text.strip()
                match = re.search(rf"dikmen/(\d{{6}})/{child_level}", href)
                if match:
                    results.append({'kode': match.group(1), 'nama': nama, 'level': child_level})
        return results

    def _parse_school_table(self, soup):
        schools = []
        if not soup: return schools
        table = soup.find("table", {"id": "table1"})
        if not table: return schools

        for row in table.find("tbody").find_all("tr"):
            cols = row.find_all("td")
            if len(cols) >= 6:
                try:
                    schools.append((
                        cols[1].text.strip(), cols[2].text.strip(), cols[5].text.strip(),
                        "DIKMEN", cols[3].text.strip(), cols[4].text.strip()
                    ))
                except: continue
        return schools

    def seed_root(self):
        print("\n🌱 Seeding Provinsi dari web...")
        soup = self._get_soup(self.root_url)
        provinces = self._parse_region_table(soup, 1)
        if provinces:
            data = [(p['kode'], p['nama'], 1, 'ROOT', 0) for p in provinces]
            with get_connection() as conn:
                conn.executemany("INSERT OR IGNORE INTO wilayah_queue VALUES (?,?,?,?,?)", data)
                conn.commit()
            print(f"✅ {len(provinces)} Provinsi siap antri.")

    def run_fase1(self):
        print("\n🚀 CRAWLER FASE 1 AKTIF!")
        with get_connection() as conn:
            if conn.execute("SELECT count(*) as t FROM wilayah_queue").fetchone()['t'] == 0:
                self.seed_root()

        while self.is_active:
            with get_connection() as conn:
                task = conn.execute("SELECT * FROM wilayah_queue WHERE status=0 ORDER BY level DESC LIMIT 1").fetchone()
            
            if not task:
                print("\n🏁 Selesai. Menunggu task baru...")
                time.sleep(2)
                continue

            kode, nama, level = task['kode'], task['nama'], task['level']
            self.live_kode = f"{nama} (L{level})"
            
            # Logger console
            indent = "  " * level
            icon = "🏢" if level == 1 else "🏙️" if level == 2 else "🌾"
            print(f"{indent}{icon} Fetch [{level}]: {nama}", end="\r")

            soup = self._get_soup(f"{self.base_url}/{kode}/{level}")
            
            if level < 3:
                children = self._parse_region_table(soup, level + 1)
                if children:
                    db_data = [(c['kode'], c['nama'], level + 1, kode, 0) for c in children]
                    with get_connection() as conn:
                        conn.executemany("INSERT OR IGNORE INTO wilayah_queue VALUES (?,?,?,?,?)", db_data)
                        conn.execute("UPDATE wilayah_queue SET status=1 WHERE kode=?", (kode,))
                        conn.commit()
                    print(f"{indent}{icon} Fetch [{level}]: {nama} -> Dapet {len(children)} anak.")
                else:
                    with get_connection() as conn:
                        conn.execute("UPDATE wilayah_queue SET status=1 WHERE kode=?", (kode,))
                        conn.commit()

            elif level == 3:
                sekolah = self._parse_school_table(soup)
                if sekolah:
                    with get_connection() as conn:
                        p1 = conn.execute("SELECT nama, parent_kode FROM wilayah_queue WHERE kode=?", (task['parent_kode'],)).fetchone()
                        kab_nama = p1['nama'] if p1 else ""
                        p2 = conn.execute("SELECT nama FROM wilayah_queue WHERE kode=?", (p1['parent_kode'],)).fetchone() if p1 else None
                        prov_nama = p2['nama'] if p2 else ""
                        
                        final_data = [s + (nama, kab_nama, prov_nama) for s in sekolah]
                        
                        conn.executemany("""INSERT OR IGNORE INTO sekolah 
                        (npsn, nama, status, bentuk_pendidikan, alamat, desa, kecamatan, kabupaten, provinsi) 
                        VALUES (?,?,?,?,?,?,?,?,?)""", final_data)
                        conn.execute("UPDATE wilayah_queue SET status=1 WHERE kode=?", (kode,))
                        conn.commit()
                    print(f"{indent}{icon} PANEN [{level}]: {nama} -> {len(sekolah)} Sekolah.")
                else:
                    with get_connection() as conn:
                        conn.execute("UPDATE wilayah_queue SET status=1 WHERE kode=?", (kode,))
                        conn.commit()
            
            time.sleep(0.5)

    # --- PARSER FASE 2 ---
    def fetch_detail(self, npsn):
        if not self.fase2_active: return
        self.live_npsn = npsn
        url = f"https://referensi.data.kemendikdasmen.go.id/tabs.php?npsn={npsn}"
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200: return
            soup = BeautifulSoup(resp.text, "html.parser")

            def g(label):
                t = soup.find("td", string=re.compile(f"^{label}$", re.I))
                if t and t.find_next_sibling("td"):
                    v = t.find_next_sibling("td")
                    if v.text.strip() == ":": v = v.find_next_sibling("td")
                    return v.get_text(strip=True) if v else ""
                return ""

            lat = re.search(r"Lintang:\s*([\-\d\.]+)", resp.text)
            lng = re.search(r"Bujur:\s*([\-\d\.]+)", resp.text)
            
            d = {
                'nlink': soup.select_one("a[href*='profil-sekolah']")['href'] if soup.select_one("a[href*='profil-sekolah']") else "",
                'bpd': g("Bentuk Pendidikan"), 'pemb': g("Kementerian Pembina"), 'naung': g("Naungan"),
                'npyp': g("NPYP"), 'skp': g("No. SK. Pendirian"), 'tglp': g("Tanggal SK. Pendirian"),
                'sko': g("Nomor SK Operasional"), 'tglo': g("Tanggal SK Operasional"),
                'skol': soup.find("a", string=re.compile("Lihat SK Operasional", re.I))['href'] if soup.find("a", string=re.compile("Lihat SK Operasional", re.I)) else "",
                'tglu': g("Tanggal Upload SK Op."),
                'akr': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)).text.strip() if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'akrl': soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I))['href'] if soup.find("a", href=re.compile("ban-pdm|satuanpendidikan", re.I)) else "",
                'luas': g("Luas Tanah"), 'list': g("Sumber Listrik"), 'fax': g("Fax"),
                'telp': g("Telepon"), 'mail': g("Email"), 'web': g("Website"), 'ops': g("Operator"),
                'lat': lat.group(1) if lat else "", 'lng': lng.group(1) if lng else "",
            }

            i1, i2 = "", ""
            itd = soup.find("td", string=re.compile("Akses Internet", re.I))
            if itd:
                i1 = itd.find_next_sibling("td").find_next_sibling("td").text.strip()
                row2 = itd.find_parent("tr").find_next_sibling("tr")
                if row2 and len(row2.find_all("td")) > 0: 
                    i2 = row2.find_all("td")[-1].text.strip()

            with get_connection() as conn:
                conn.execute("""UPDATE sekolah SET 
                    npsn_link=?, bentuk_pendidikan_detail=?, kementerian_pembina=?, naungan=?, npyp=?, 
                    sk_pendirian=?, tgl_sk_pendirian=?, sk_operasional=?, tgl_sk_operasional=?, 
                    sk_operasional_link=?, tgl_upload_sk=?, akreditasi=?, akreditasi_link=?, 
                    luas_tanah=?, internet_1=?, internet_2=?, sumber_listrik=?, fax=?, telepon=?, 
                    email=?, website=?, operator=?, lintang=?, bujur=?, fase2_done=1 WHERE npsn=?""", 
                    (d['nlink'], d['bpd'], d['pemb'], d['naung'], d['npyp'], d['skp'], d['tglp'], d['sko'], d['tglo'], d['skol'], d['tglu'], d['akr'], d['akrl'],
                     d['luas'], i1, i2, d['list'], d['fax'], d['telp'], d['mail'], d['web'], d['ops'], d['lat'], d['lng'], npsn))
                conn.commit()
            print(f"  [Fase 2] ✅ NPSN: {npsn} OK.", end="\r")
        except: pass

    def run_fase2(self):
        print("\n🚀 CRAWLER FASE 2 AKTIF!")
        self.fase2_active = True
        while self.fase2_active:
            with get_connection() as conn:
                todo = conn.execute("SELECT npsn FROM sekolah WHERE fase2_done=0 LIMIT 50").fetchall()
            if not todo: 
                print("\n🏁 Fase 2 Selesai. Semua detail sudah diambil.")
                break
            
            for row in todo:
                if not self.fase2_active: break
                self.fetch_detail(row['npsn'])
                time.sleep(0.2)
                
        self.fase2_active = False

crawler_instance = KemendikCrawler()