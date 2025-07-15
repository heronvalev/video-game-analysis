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

def split_categories(category_str):
    if pd.isna(category_str):
        return []
    return category_str.split(";")

# Parse semicolon-separated category strings into lists
metadata_df["categories_list"] = metadata_df["categories"].apply(split_categories)

# Collect all unique category names across games
all_categories = set()

for categories in metadata_df["categories_list"]:
    for cat in categories:
        cat = cat.strip()
        if cat:
            all_categories.add(cat)

# Create a lookup dictionary: category name to numeric ID
category_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_categories))}

# Convert the category lookup dictionary into a DataFrame for export
categories_df = pd.DataFrame([
    {"category_id": cid, "category_name": name}
    for name, cid in category_to_id.items()
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

# Merge description into metadata using matching game IDs
games_df = metadata_df.merge(descr_df, left_on="appid", right_on="steam_appid", how="left")

# Remove duplicate key column after merge
games_df.drop(columns=["steam_appid"], inplace=True)

# Select final subset of columns for export to SQLite
games_df = games_df[[
    "appid",
    "name",
    "release_date",
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

conn.close()