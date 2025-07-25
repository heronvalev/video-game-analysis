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

# cleaner_steam = SteamDataCleaner(dataframes["steam"])
# list_of_columns = cleaner_steam.df.columns.values.tolist()

# clean_df_object = (
#     cleaner_steam
#     .standardise_columns()
#     .drop_duplicates()
#     .fill_missing(list_of_columns)
#     .clean_text_column(['name', 'developer', 'publisher', 'platforms', 'categories', 'genres', 'steamspy_tags'])
#     .convert_to_numeric(['english', 'required_age', 'achievements', 'positive_ratings', 'negative_ratings', 'average_playtime', 'median_playtime', 'price'])
#     .convert_to_datetime('release_date')
# )
# clean_df_object.print_log_summary()
# clean_df = clean_df_object.get_df()

# save_cleaned_csv(clean_df, 'steam_data_cleaned.csv')



# cleaner_media = SteamDataCleaner(dataframes["steam_media_data"])

# clean_df_object = (
#     cleaner_media
#     .standardise_columns()
#     .drop_duplicates()
# )

# clean_df_object.print_log_summary()
# clean_df = clean_df_object.get_df()

# columns_to_drop = ['screenshots', 'background', 'movies']
# clean_df.drop(columns=columns_to_drop, axis="columns", inplace=True)

# save_cleaned_csv(clean_df, 'steam_media_cleaned.csv')


# cleaner_steamspy_tag = SteamDataCleaner(dataframes["steamspy_tag_data"])

# list_of_all_columns = cleaner_steamspy_tag.df.columns.values.tolist()
# list_of_columns = list_of_all_columns[1:] #removing the app_id column from the list as it is the main identifier

# clean_df_object = (
#     cleaner_steamspy_tag
#     .fill_missing(columns=list_of_columns)
#     .convert_to_numeric(columns=list_of_columns)
# )

# clean_df_object.print_log_summary()
# clean_df = clean_df_object.get_df()

# save_cleaned_csv(clean_df,'steamspy_tag_data_cleaned.csv')

