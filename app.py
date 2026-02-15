from flask import Flask, render_template, redirect, jsonify, request
from threading import Thread
from db import init_db, get_connection
from crawler import crawler_instance

app = Flask(__name__)

@app.route("/")
def index(): return render_template("index.html")

@app.route("/enrichment")
def enrichment(): return render_template("enrichment.html")

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
    total_filtered = conn.execute(q.replace("SELECT *", "SELECT COUNT(*) as t")).fetchone()['t']
    conn.close()
    return render_template("viewer.html", rows=rows, total_data=total_all, total_enriched=total_enriched, pending=(total_all-total_enriched), page=page, total_pages=(total_filtered//per_page)+1, search=search, view_filter=view_filter)

@app.route("/api/stats")
def stats():
    conn = get_connection()
    t = conn.execute("SELECT COUNT(*) as t FROM sekolah").fetchone()['t']
    f2 = conn.execute("SELECT COUNT(*) as t FROM sekolah WHERE fase2_done=1").fetchone()['t']
    p = conn.execute("SELECT * FROM progress WHERE id=1").fetchone()
    conn.close()
    return jsonify({"total": t, "fase2_done": f2, "is_running": crawler_instance.is_active, "fase2_running": crawler_instance.fase2_active, "live_kode": crawler_instance.live_kode, "live_npsn": crawler_instance.live_npsn, "progress": {"p": p['prov'], "k": p['kab'], "kc": p['kec']}})

@app.route("/start")
def start():
    conn = get_connection(); conn.execute("UPDATE progress SET is_running=1 WHERE id=1"); conn.commit(); conn.close()
    if not crawler_instance.is_active: Thread(target=crawler_instance.run_f1, daemon=True).start()
    return redirect("/")

@app.route("/pause")
def pause():
    conn = get_connection(); conn.execute("UPDATE progress SET is_running=0 WHERE id=1"); conn.commit(); conn.close()
    crawler_instance.is_active = False
    return redirect("/")

@app.route("/fase2/start")
def fase2_start():
    conn = get_connection(); conn.execute("UPDATE progress SET fase2_running=1 WHERE id=1"); conn.commit(); conn.close()
    if not crawler_instance.fase2_active: Thread(target=crawler_instance.run_f2, daemon=True).start()
    return redirect("/enrichment")

@app.route("/fase2/pause")
def fase2_pause():
    conn = get_connection(); conn.execute("UPDATE progress SET fase2_running=0 WHERE id=1"); conn.commit(); conn.close()
    crawler_instance.fase2_active = False
    return redirect("/enrichment")

if __name__ == "__main__":
    init_db(); app.run(debug=True, port=5000)