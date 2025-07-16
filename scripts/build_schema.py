import pandas as pd
import sqlite3
import os

# Set dynamic paths to processed data and output database
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data", "processed"))
DB_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "data", "steam.sqlite"))

# Specify input files for cleaned metadata and game descriptions
metadata_file = os.path.normpath(os.path.join(DATA_DIR, "steam_data_cleaned.csv"))
descr_file = os.path.normpath(os.path.join(DATA_DIR, "steam_description_data_cleaned.csv"))

# Load CSVs into DataFrames
metadata_df = pd.read_csv(metadata_file)
descr_df = pd.read_csv(descr_file)

def split_string(str_to_split):
    if pd.isna(str_to_split):
        return []
    return str_to_split.split(";")

# Parse columns with semicolon-separated strings into lists
metadata_df["categories_list"] = metadata_df["categories"].apply(split_string)

metadata_df["genres_list"] = metadata_df["genres"].apply(split_string)

metadata_df["platforms_list"] = metadata_df["platforms"].apply(split_string)

# Collect all unique category names across games
all_categories = set()

for categories in metadata_df["categories_list"]:
    for cat in categories:
        cat = cat.strip()
        if cat:
            all_categories.add(cat)

# Repeat the latter for unique genres accross games
all_genres = set()

for genres in metadata_df["genres_list"]:
    for genre in genres:
        genre = genre.strip()
        if genre:
            all_genres.add(genre)

# Repeat the latter for each game platform
all_platforms = set()

for plats in metadata_df["platforms_list"]:
    for p in plats:
        p = p.strip()
        if p:
            all_platforms.add(p)

# Create a lookup dictionary: category name to numeric ID
category_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_categories))}

# Create a lookup dictionary: genre name to numeric ID
genre_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_genres))}

# Create a lookup dictionary: platform name to numeric ID
platform_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_platforms))}

# Convert the category lookup dictionary into a DataFrame for export
categories_df = pd.DataFrame([
    {"category_id": cid, "category_name": name}
    for name, cid in category_to_id.items()
])

# Repeat the latter for the genre lookup dictionary
genres_df = pd.DataFrame([
    {"genre_id": gid, "genre_name": name}
    for name, gid in genre_to_id.items()
])

# Repeat the latter for the platform lookup dictionary
platforms_df = pd.DataFrame([
    {"platform_id": pid, "platform_name": name}
    for name, pid in platform_to_id.items()
])

# Construct the many-to-many junction table linking each game to its category IDs
game_categories_records = []

for _, row in metadata_df.iterrows():
    appid = row["appid"]
    for cat in row["categories_list"]:
        cat = cat.strip()
        if cat:
            category_id = category_to_id[cat]
            game_categories_records.append((appid, category_id))

game_categories_df = pd.DataFrame(
    game_categories_records,
    columns=["appid", "category_id"]
)

# Repeat the latter for each game to its genre IDs
game_genres_records = []

for _, row in metadata_df.iterrows():
    appid = row["appid"]
    for genre in row["genres_list"]:
        genre = genre.strip()
        if genre:
            genre_id = genre_to_id[genre]
            game_genres_records.append((appid, genre_id))

game_genres_df = pd.DataFrame(
    game_genres_records,
    columns=["appid", "genre_id"]
)

# Repeat the latter for each platform to its platform IDs
game_platforms_records = []

for _, row in metadata_df.iterrows():
    appid = row["appid"]
    for platform in row["platforms_list"]:
        platform = platform.strip()
        if platform:
            pid = platform_to_id[platform]
            game_platforms_records.append((appid, pid))

game_platforms_df = pd.DataFrame(
    game_platforms_records,
    columns=["appid", "platform_id"]
)

# Merge description into metadata using matching game IDs
games_df = metadata_df.merge(descr_df, left_on="appid", right_on="steam_appid", how="left")

# Remove duplicate key column after merge
games_df.drop(columns=["steam_appid"], inplace=True)

# Select final subset of columns for export to SQLite
games_df = games_df[[
    "appid",
    "name",
    "release_date",
    "developer",
    "publisher",
    "short_description",
    "price"
]]

ratings_df = metadata_df[[
    "appid",
    "positive_ratings",
    "negative_ratings",
    "average_playtime",
    "median_playtime",
    "owners",
    "achievements",
    "required_age"
]]

# Connect to database and export DataFrame as table
conn = sqlite3.connect(DB_PATH)

games_df.to_sql("games", conn, index=False, if_exists="replace")

ratings_df.to_sql("ratings", conn, index=False, if_exists="replace")

categories_df.to_sql("categories", conn, index=False, if_exists="replace")
game_categories_df.to_sql("game_categories", conn, index=False, if_exists="replace")

genres_df.to_sql("genres", conn, index=False, if_exists="replace")
game_genres_df.to_sql("game_genres", conn, index=False, if_exists="replace")

platforms_df.to_sql("platforms", conn, index=False, if_exists="replace")
game_platforms_df.to_sql("game_platforms", conn, index=False, if_exists="replace")


conn.close()