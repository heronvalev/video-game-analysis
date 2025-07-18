from app import app
from flask import render_template, request
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join("data", "steam.sqlite")

def query_games(search_term=""):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
        SELECT g.appid, g.name, g.release_date, gm.header_image
        FROM games g
        LEFT JOIN game_media gm ON g.appid = gm.appid
        WHERE g.name LIKE ?
        LIMIT 20                   
    """, (f"%{search_term}%",))

    results = cursor.fetchall()
    conn.close()
    return results

    

@app.route("/", methods = ["GET", "POST"])
def home():
    method = request.method
    game_list = []
    if request.method == "POST":
        query = request.form.get("search", "")
        game_list = query_games(query)
    return render_template("index.html", results=game_list, req_method=method, current_year=datetime.now().year)