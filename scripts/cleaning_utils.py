import pandas as pd
from bs4 import BeautifulSoup

class SteamDataCleaner:
    """
    A utility class for cleaning Steam dataset CSVs using standardized methods.
    Designed for flexible, reusable data cleaning across different sources.
    """
    def __init__(self, df):
        """
        Initialize the cleaner with a Pandas DataFrame.
        
        Parameters:
        df (pd.DataFrame): The raw dataframe to be cleaned.
        """
        self.df = df.copy()
    
    def standardise_columns(self):
        """
        Convert column names to lowercase, strip whitespace, and replace spaces with underscores.
        
        Returns:
        self: Enables method chaining.
        """
        self.df.columns = self.df.columns.str.strip().str.lower().str.replace(' ', '_')
        return self
    
    def drop_duplicates(self):
        """
        Remove duplicate rows from the dataframe in-place.
        
        Returns:
        self: Enables method chaining.
        """
        self.df.drop_duplicates(inplace=True)
        return self
    
    def fill_missing(self, column, value='Unknown'):
        """
        Fill missing values in a specified column with a given value.
        
        Parameters:
        column (str): Column to fill missing values in.
        value (str): Value to replace missing entries with. Default is 'Unknown'.
        
        Returns:
        self: Enables method chaining.
        """
        if column in self.df.columns:
            self.df[column] = self.df[column].fillna(value)
        return self

    def clean_text_column(self, column):
        """
        Normalize text in a specified column: strip whitespace, remove newlines, and convert to lowercase.
        
        Parameters:
        column (str): The column to clean text values for.
        
        Returns:
        self: Enables method chaining.
        """
        if column in self.df.columns:
            self.df[column] = (
                self.df[column]
                .astype(str)
                .str.strip()
                .str.replace('\n', ' ')
                .str.lower()
            )
        return self

    def remove_html_from_column(self, column):
        """
        Strip HTML tags from a specified column using BeautifulSoup.

        Parameters:
        column (str): Name of the column containing HTML content.

        Returns:
        self
        """

        def strip_html(text):
            """
            Convert raw HTML content into plain text.
            """
            # Safely convert value to string in case it's None or not a string
            soup = BeautifulSoup(str(text), "html.parser")
            
            # Extract text content, remove tags, and clean whitespace
            return soup.get_text(separator=' ', strip=True)

        if column in self.df.columns:
            # Apply the strip_html function to each cell in the column
            self.df[column] = self.df[column].apply(strip_html)

        return self
    
    def convert_to_numeric(self, column):
        """
        Convert a column to numeric, coercing errors and filling NaNs with zero.
        
        Parameters:
        column (str): The column to convert.
        
        Returns:
        self: Enables method chaining.
        """
        if column in self.df.columns:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce').fillna(0)
        return self
    
    def convert_to_datetime(self, column):
        """
        Convert a column to datetime format, coercing errors.
        
        Parameters:
        column (str): The column to convert.
        
        Returns:
        self: Enables method chaining.
        """
        if column in self.df.columns:
            self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
        return self

    def get_df(self):
        """
        Retrieve the cleaned DataFrame.
        
        Returns:
        pd.DataFrame: The cleaned dataframe.
        """
        return self.df