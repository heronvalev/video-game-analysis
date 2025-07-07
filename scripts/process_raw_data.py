import os
import pandas as pd
from cleaning_utils import SteamDataCleaner

# List of .csv file names

csv_files = [
    'steam_description_data.csv',
    'steam_media_data.csv',
    'steam.csv',
    'steamspy_tag_data.csv'
]

# Get current os directory

current_dir = os.path.dirname(os.path.abspath(__file__))

# Generate a dynamic path for the .csv files and a dictionary with the dataframes

dataframes = {}

for file_name in csv_files:
    csv_path = os.path.normpath(os.path.join(current_dir, '..', 'data', 'raw', file_name))
    df = pd.read_csv(csv_path)

    key = file_name.replace('.csv', '')
    dataframes[key] = df

# Function to save and export processed .csv files

def save_cleaned_csv(df, file_name):
    """
    Saves a cleaned DataFrame to the 'processed' folder as a CSV file.

    Parameters:
    - df: pandas DataFrame to save
    - file_name: name for the output CSV
    """
    processed_dir = os.path.normpath(os.path.join(current_dir, '..', 'data', 'processed'))
    file_path = os.path.join(processed_dir, file_name)

    df.to_csv(file_path, index=False)
    print(f"Saved cleaned data to: {file_path}")

# Brief raw data exploration

for df_name, df in dataframes.items():
    print(f"DataFrame name: {df_name}")
    print("\n")
    print(df.head())
    print(df.info())
    print("\n")
    print("\n")
    print("\n")

# Use the SteamDataCleaner class to clean the 4 DataFrame objects and comment them out once executed

# cleaner_description = SteamDataCleaner(dataframes["steam_description_data"])

# clean_df = (
#     cleaner_description
#     .standardise_columns()
#     .drop_duplicates()
#     .remove_html_from_column("short_description")
#     .clean_text_column("short_description")
#     .fill_missing("short_description","No available description")
#     .get_df()
# )
# columns_to_drop = ["detailed_description", "about_the_game"]
# clean_df.drop(columns=columns_to_drop, axis="columns", inplace=True)

# save_cleaned_csv(clean_df, 'steam_description_data_cleaned.csv')

