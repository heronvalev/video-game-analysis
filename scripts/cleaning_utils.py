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
        self.log = []
    
    # Return self with the following methods to allow method chaining

    def standardise_columns(self):
        """
        Convert column names to lowercase, strip whitespace, and replace spaces with underscores.
        
        Returns:
        self
        """
        self.df.columns = self.df.columns.str.strip().str.lower().str.replace(' ', '_')
        return self
    
    def drop_duplicates(self):
        """
        Remove duplicate rows from the dataframe in-place and log the number removed.
        
        Returns:
        self
        """
        before = len(self.df)
        self.df.drop_duplicates(inplace=True)
        after = len(self.df)
        dropped = before - after

        if dropped > 0:
            self.log.append(f"Dropped {dropped} duplicate rows.")

        return self
    
    def fill_missing(self, columns, value='Unknown'):
        """
        Fill missing values in one or more columns in a df with a given value.
        
        Parameters:
        columns (str or list): Column(s) to fill missing values in.
        value (str): Value to replace missing entries with. Default is 'Unknown'.
        
        Returns:
        self
        """
        # Convert single-column str value to a list to pass to the method
        if isinstance(columns, str):
            columns = [columns]
        
        # Apply changes to all columns specified
        for column in columns:
            if column in self.df.columns:
                before = self.df[column].isna().sum()
                self.df[column] = self.df[column].fillna(value)
                filled = before - self.df[column].isna().sum()

                # Log any changes made
                if filled > 0:
                    self.log.append(f"Filled {filled} missing values in '{column}' with '{value}'.")

        return self

    def clean_text_column(self, columns):
        """
        Clean text in one or more columns: strip whitespace, remove newlines, and convert to lowercase.
        
        Parameters:
        column (str or list): The column(s) to clean text values for.
        
        Returns:
        self
        """
        if isinstance(columns, str):
            columns = [columns]
        
        for column in columns:
            if column in self.df.columns:
                before = self.df[column].copy()
                cleaned = (
                    self.df[column]
                    .astype(str)
                    .str.strip()
                    .str.replace('\n', ' ', regex = False)
                    .str.lower()
                )
                changed = (cleaned != before).sum()
                if changed > 0:
                    self.log.append(f"Cleaned text in '{column}': {changed} rows affected.")
                
        return self

    def remove_html_from_column(self, columns):
        """
        Strip HTML tags from one or more columns using BeautifulSoup.

        Parameters:
        column (str or list): Column(s) containing HTML content.

        Returns:
        self
        """
        if isinstance(columns, str):
            columns = [columns]

        def strip_html(text):
            """
            Convert raw HTML content into plain text.
            """
            # Safely convert value to string in case it's None or not a string
            soup = BeautifulSoup(str(text), "html.parser")
            
            # Extract text content, remove tags, and clean whitespace
            return soup.get_text(separator=' ', strip=True)
        
        for column in columns:
            if column in self.df.columns:
                # Apply the strip_html function to each cell in the column
                before = self.df[column].copy()
                stripped = self.df[column].apply(strip_html)
                changed = (stripped != before).sum()
                if changed > 0:
                    self.log.append(f"Removed HTML from '{column}': {changed} rows cleaned.")

        return self
    
    def convert_to_numeric(self, columns):
        """
        Convert one or more columns to numeric, coercing errors and filling NaNs with zero.
        
        Parameters:
        column (str or list): The column(s) to convert.
        
        Returns:
        self
        """
        if isinstance(columns, str):
            columns = [columns]

        for column in columns:
            if column in self.df.columns:
                # Coerce to numeric - invalid entries become NaN
                converted = pd.to_numeric(self.df[column], errors='coerce')
                new_nans = converted.isna().sum()

                # Replace NaNs with 0 and assign back to original DataFrame
                self.df[column] = converted.fillna(0)

                if new_nans > 0:
                    self.log.append(f"Converted '{column}' to numeric: {new_nans} entries replaced with 0.")

        return self
    
    def convert_to_datetime(self, columns):
        """
        Convert one or more columns to datetime format, coercing errors.
        
        Parameters:
        column (str or list): The column(s) to convert.
        
        Returns:
        self
        """
        if isinstance(columns, str):
            columns = [columns]

        for column in columns:
            if column in self.df.columns:
                # Count missing values before conversion
                missing_before = self.df[column].isna().sum()

                # Convert to datetime - invalid values become NaT
                converted = pd.to_datetime(self.df[column], errors='coerce')

                # Count new NaT values introduced by failed conversions
                missing_after = converted.isna().sum()
                new_nats = missing_after - missing_before

                # Assign converted column back to the DataFrame
                self.df[column] = converted

                if new_nats > 0:
                    self.log.append(f"Converted '{column}' to datetime: {new_nats} entries became NaT.")


        return self

    def get_df(self):
        """
        Retrieve the cleaned DataFrame.
        
        Returns:
        pd.DataFrame: The cleaned dataframe.
        """
        return self.df
    
    def get_log(self):
        """
        Retrieve the log of cleaning performed.

        Returns:
        list: A list of all data cleaning transformations.
        """
        return self.log
    
    def print_log_summary(self):
        """
        Print a formatted summary of all logged cleaning actions.

        Outputs:
        Console printout of each log entry.
        """
        print("\n Cleaning Summary:")
        for entry in self.log:
            print(f"• {entry}")

