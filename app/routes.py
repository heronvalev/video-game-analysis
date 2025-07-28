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

    
# Home Page
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

# Game Details Page based on appid
@app.route("/details/<int:appid>")
def game_details(appid):

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Basic game info and ratings
    cursor.execute("""
        SELECT g.appid, g.name, g.release_date, g.developer, g.publisher,
            g.short_description, g.price, g.english, gm.header_image,
            r.positive_ratings, r.negative_ratings, r.average_playtime,
            r.median_playtime, r.owners, r.achievements, r.required_age
        FROM games g
        LEFT JOIN game_media gm ON g.appid = gm.appid
        LEFT JOIN ratings r ON g.appid = r.appid
        WHERE g.appid = ?
    """, (appid,))
    game = cursor.fetchone()
    
    # Categories
    cursor.execute("""
        SELECT c.category_name
        FROM categories c
        JOIN game_categories gc ON c.category_id = gc.category_id
        WHERE gc.appid = ?
    """, (appid,))
    categories = [row['category_name'] for row in cursor.fetchall()]

    # Genres
    cursor.execute("""
        SELECT g.genre_name
        FROM genres g
        JOIN game_genres gg ON g.genre_id = gg.genre_id
        WHERE gg.appid = ?
    """, (appid,))
    genres = [row['genre_name'] for row in cursor.fetchall()]

    # Platforms
    cursor.execute("""
        SELECT p.platform_name
        FROM platforms p
        JOIN game_platforms gp ON p.platform_id = gp.platform_id
        WHERE gp.appid = ?
    """, (appid,))
    platforms = [row['platform_name'].capitalize() for row in cursor.fetchall()]

    # SteamSpy Tags
    cursor.execute("""
        SELECT t.tag_name, v.vote_count
        FROM steamspy_tags t
        JOIN steamspy_tag_votes v ON t.tag_id = v.tag_id
        WHERE v.appid = ?
        ORDER BY v.vote_count DESC
    """, (appid,))
    steamspy_tags = cursor.fetchall()

    conn.close()

    if game is None:
        return render_template("404.html"), 404
    
    # Convert to a python dictionary
    formatted_game = dict(game)

    # Format release date
    date_obj = datetime.strptime(game["release_date"], "%Y-%m-%d")
    formatted_game["release_date"] = date_obj.strftime("%d %B, %Y")

    # Format English support
    formatted_game["english"] = "Yes" if game["english"] else "No"

    # Derived stats
    pos = game["positive_ratings"] or 0
    neg = game["negative_ratings"] or 0
    total_ratings = pos + neg

    ## Calculate positive rating percentage
    if total_ratings > 0:
        positive_pct = round((pos / total_ratings) * 100)
    else:
        positive_pct = "Not available"

    # Format average and median playtime (convert to hours)
    avg_playtime = game["average_playtime"]
    median_playtime = game["median_playtime"]

    def format_playtime(minutes):
        if minutes and minutes > 0:
            hours = minutes / 60
            return f"{hours:.1f} hrs"
        return "Not available"
    
    avg_playtime_fmt = format_playtime(avg_playtime)
    median_playtime_fmt = format_playtime(median_playtime)

    # Format the owners values
    owners_text = game["owners"] if game["owners"] else "Not available"

    # Achievements and required age (just show raw values or N/A)
    achievements = game["achievements"] or "No achievements"
    required_age = game["required_age"] if game["required_age"] and game["required_age"] > 0 else "None"

    formatted_game.update({
    "total_ratings": total_ratings,
    "positive_pct": positive_pct,
    "avg_playtime_fmt": avg_playtime_fmt,
    "median_playtime_fmt": median_playtime_fmt,
    "owners_text": owners_text,
    "achievements": achievements,
    "required_age": required_age
    })

    return render_template("details.html", 
                           game=formatted_game,
                           platforms=platforms,
                           genres=genres,
                           categories=categories,
                           tags=steamspy_tags, 
                           current_year = datetime.now().year)
