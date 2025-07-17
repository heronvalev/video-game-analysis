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
votes_file = os.path.normpath(os.path.join(DATA_DIR, "steamspy_tag_data_cleaned.csv"))
media_file = os.path.join(DATA_DIR, "steam_media_cleaned.csv")

# Load CSVs into DataFrames
metadata_df = pd.read_csv(metadata_file)
descr_df = pd.read_csv(descr_file)
votes_df = pd.read_csv(votes_file)
media_df = pd.read_csv(media_file)

# Create a function to split semicolon-delimited strings
def split_string(str_to_split):
    if pd.isna(str_to_split):
        return []
    return str_to_split.split(";")

# Parse columns with semicolon-separated strings into lists
metadata_df["categories_list"] = metadata_df["categories"].apply(split_string)

metadata_df["genres_list"] = metadata_df["genres"].apply(split_string)

metadata_df["platforms_list"] = metadata_df["platforms"].apply(split_string)

metadata_df["steamspy_tags_list"] = metadata_df["steamspy_tags"].apply(split_string)

# Collect all unique values across games
all_categories = set()

for categories in metadata_df["categories_list"]:
    for cat in categories:
        cat = cat.strip()
        if cat:
            all_categories.add(cat)


all_genres = set()

for genres in metadata_df["genres_list"]:
    for genre in genres:
        genre = genre.strip()
        if genre:
            all_genres.add(genre)


all_platforms = set()

for plats in metadata_df["platforms_list"]:
    for p in plats:
        p = p.strip()
        if p:
            all_platforms.add(p)
## Tags from semicolon-separated steamspy_tags column
metadata_tags = set()
for tags in metadata_df["steamspy_tags_list"]:
    for tag in tags:
        tag = tag.strip()
        if tag:
            metadata_tags.add(tag)

## Tags from votes CSV (column names, excluding 'appid')
csv_tags = set(votes_df.columns) - {"appid"}

## Combine tags from both sources
all_tags = metadata_tags.union(csv_tags)

# Create a lookup dictionary: value name to numeric ID
category_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_categories))}

genre_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_genres))}

platform_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_platforms))}

tag_to_id = {name: idx + 1 for idx, name in enumerate(sorted(all_tags))}

# Convert the lookup dictionaries into a DataFrames for export
categories_df = pd.DataFrame([
    {"category_id": cid, "category_name": name}
    for name, cid in category_to_id.items()
])

genres_df = pd.DataFrame([
    {"genre_id": gid, "genre_name": name}
    for name, gid in genre_to_id.items()
])

platforms_df = pd.DataFrame([
    {"platform_id": pid, "platform_name": name}
    for name, pid in platform_to_id.items()
])

tags_df = pd.DataFrame([
    {"tag_id": tid, "tag_name": name}
    for name, tid in tag_to_id.items()
])

# Construct the many-to-many junction tables linking each game to its category, genre, platform and tag IDs
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


game_tags_records = []

for _, row in metadata_df.iterrows():
    appid = row["appid"]
    for tag in row["steamspy_tags_list"]:
        tag = tag.strip()
        if tag:
            t_id = tag_to_id[tag]
            game_tags_records.append((appid, t_id))

game_steamspy_tags_df = pd.DataFrame(
    game_tags_records,
    columns=["appid", "steamspy_tag_id"]
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
    "english",
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

# Melt the wide votes_df into a long table df
votes_long_df = votes_df.melt(
    id_vars="appid",
    var_name="tag_name",
    value_name="vote_count"
)
# Drop entries where a game has 0 votes for a tag
votes_nonzero_df = votes_long_df[votes_long_df["vote_count"] > 0]

# Merge tags_df with votes_nonzero_df on "tag_name" column
votes_with_id_df = votes_nonzero_df.merge(
    tags_df,
    on="tag_name",
    how="left"
)

# Select final subset of columns for steamspy votes to export to SQLite
steamspy_votes_df = votes_with_id_df[[
    "appid", 
    "tag_id", 
    "vote_count"
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

tags_df.to_sql("steamspy_tags", conn, index=False, if_exists="replace")
game_steamspy_tags_df.to_sql("game_steamspy_tags", conn, index=False, if_exists="replace")

steamspy_votes_df.to_sql("steamspy_tag_votes", conn, index=False, if_exists="replace")

media_df.to_sql("game_media", conn, index=False, if_exists="replace")

conn.close()