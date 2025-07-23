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
    transparent_navbar = True

    if request.method == "POST":

        query = request.form.get("search", "")
        raw_results = query_games(query)

        # Format the release date from DB
        formatted_results = []
        for game in raw_results:
            date_obj = datetime.strptime(game["release_date"], "%Y-%m-%d")
            formatted_date = date_obj.strftime("%B %Y")

            game_dict = dict(game)
            game_dict["release_date"] = formatted_date
            formatted_results.append(game_dict)
        
        game_list = formatted_results
        transparent_navbar = False

    return render_template("index.html", results=game_list, req_method=method, tr_navbar=transparent_navbar, current_year=datetime.now().year)
