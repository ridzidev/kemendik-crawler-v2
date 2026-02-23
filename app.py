import logging
from flask import Flask, render_template, redirect, jsonify, request
from threading import Thread
from db import init_db, get_connection

# Langsung import instance yang sudah dibuat di crawler.py
from crawler import crawler_instance

# --- MATIKAN LOG FLASK YANG BERISIK ---
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

@app.route("/")
def index(): 
    return render_template("index.html")

@app.route("/enrichment")
def enrichment(): 
    return render_template("enrichment.html")

@app.route("/viewer")
def viewer():
    search = request.args.get('search', '')
    page = request.args.get('page', 1, type=int)
    view_filter = request.args.get('filter', 'all')
    per_page = 100
    offset = (page - 1) * per_page
    conn = get_connection()
    total_all = conn.execute("SELECT COUNT(*) as t FROM sekolah").fetchone()['t']
    total_enriched = conn.execute("SELECT COUNT(*) as t FROM sekolah WHERE fase2_done=1").fetchone()['t']
    
    q = "SELECT * FROM sekolah WHERE 1=1 "
    if view_filter == 'enriched': q += "AND fase2_done=1 "
    if search: q += f"AND (npsn LIKE '%{search}%' OR nama LIKE '%{search}%') "
    
    rows = conn.execute(q + "ORDER BY id DESC LIMIT ? OFFSET ?", (per_page, offset)).fetchall()
    
    q_count = q.replace("SELECT *", "SELECT COUNT(*) as t")
    total_filtered = conn.execute(q_count).fetchone()['t']
    conn.close()
    
    total_pages = (total_filtered // per_page) + 1
    return render_template("viewer.html", rows=rows, total_data=total_all, total_enriched=total_enriched, pending=(total_all-total_enriched), page=page, total_pages=total_pages, search=search, view_filter=view_filter)

@app.route("/api/stats")
def stats():
    conn = get_connection()
    total_sekolah = conn.execute("SELECT COUNT(*) as t FROM sekolah").fetchone()['t']
    
    # Hitung Progress Hirarki dari Queue
    q_stats = {}
    for lvl in [1, 2, 3]:
        done = conn.execute(f"SELECT COUNT(*) as t FROM wilayah_queue WHERE level={lvl} AND status=1").fetchone()['t']
        total = conn.execute(f"SELECT COUNT(*) as t FROM wilayah_queue WHERE level={lvl}").fetchone()['t']
        q_stats[f"l{lvl}"] = {"done": done, "total": total}

    current_task = conn.execute("SELECT * FROM wilayah_queue WHERE status=0 ORDER BY level DESC LIMIT 1").fetchone()
    conn.close()
    
    task_info = {"kode": "-", "nama": "Idle / Selesai", "level": "-"}
    if current_task:
        task_info = {"kode": current_task['kode'], "nama": current_task['nama'], "level": current_task['level']}

    return jsonify({
        "total": total_sekolah,
        "is_running": crawler_instance.is_active,
        "fase2_running": crawler_instance.fase2_active,
        "queue": q_stats,
        "current_task": task_info,
        "live_kode": crawler_instance.live_kode,
        "live_npsn": crawler_instance.live_npsn
    })

@app.route("/start")
def start():
    if not crawler_instance.is_active: 
        crawler_instance.is_active = True
        Thread(target=crawler_instance.run_fase1, daemon=True).start()
    return redirect("/")

@app.route("/pause")
def pause():
    crawler_instance.is_active = False
    return redirect("/")

@app.route("/fase2/start")
def fase2_start():
    if not crawler_instance.fase2_active: 
        crawler_instance.fase2_active = True
        Thread(target=crawler_instance.run_fase2, daemon=True).start()
    return redirect("/enrichment")

@app.route("/fase2/pause")
def fase2_pause():
    crawler_instance.fase2_active = False
    return redirect("/enrichment")

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000)