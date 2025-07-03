import os
import pandas as pd

csv_files = [
    'steam_description_data.csv',
    'steam_media_data.csv',
    'steam.csv',
    'steamspy_tag_data.csv'
]

current_dir = os.path.dirname(os.path.abspath(__file__))

dataframes = {}

for file_name in csv_files:
    csv_path = os.path.normpath(os.path.join(current_dir, '..', 'data', 'raw', file_name))
    df = pd.read_csv(csv_path)

    key = file_name.replace('.csv', '')
    dataframes[key] = df

for df_name, df in dataframes.items():
    print(f"DataFrame name: {df_name}")
    print("\n")
    print(df.head())
    print(df.info())
    print("\n")
    print("\n")
    print("\n")