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

# Connect to database and export DataFrame as table

conn = sqlite3.connect(DB_PATH)

games_df.to_sql("games", conn, index=False, if_exists="replace")

conn.close()