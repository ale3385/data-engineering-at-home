import pandas as pd
import os
import re
from datetime import datetime

# Extraction Functions
def load_csv(file_path):
    """
    Load a CSV file into a Pandas DataFrame.
    Args:
        file_path (str): The path to the CSV file.
    Returns:
        DataFrame: A Pandas DataFrame containing the data from the CSV file.
    """
    return pd.read_csv(file_path)

def load_json(file_path):
    """
    Load a JSON file into a Pandas DataFrame.
    Args:
        file_path (str): The path to the JSON file.
    Returns:
        DataFrame: A Pandas DataFrame containing the data from the JSON file.
    """
    return pd.read_json(file_path)

# Data Transformation Functions
def clean_data(df):
    """
    Clean the data in a DataFrame by removing duplicates and handling missing values.
    Converts non-hashable types (like lists and dicts) to strings for duplicate removal.
    Args:
        df (DataFrame): The DataFrame to clean.
    Returns:
        DataFrame: The cleaned DataFrame.
    """
    for col in df.columns:
        if isinstance(df[col].iloc[0], (list, dict)):
            df[col] = df[col].apply(lambda x: str(x))
    df = df.drop_duplicates()
    df = df.dropna()
    return df

# Save DataFrame to CSV Function
def save_partitioned_csv(df, file_name):
    """
    Save a DataFrame to a CSV file, partitioned by the current date and time, in an 'output' folder.
    Args:
        df (DataFrame): The DataFrame to save.
        file_name (str): The base name for the file.
    """
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_folder = os.path.join('output', now)
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    output_path = os.path.join(output_folder, f'{file_name}.csv')
    df.to_csv(output_path, index=False)
    print(f'Saved file to {output_path}')

# Function to Flatten Dictionaries in DataFrame's Column
def flatten_json(df, column_name):
    """
    Flatten dictionaries contained in a specific column of a DataFrame.
    Args:
        df (DataFrame): The DataFrame containing dictionaries.
        column_name (str): The name of the column containing dictionaries.
    Returns:
        DataFrame: A DataFrame with dictionaries flattened into separate columns.
    """
    if isinstance(df[column_name].iloc[0], dict):
        return df.drop(columns=[column_name]).join(pd.json_normalize(df[column_name]))
    else:
        return df

# Function to Clean and Validate CSV Data
def clean_csv_data(df, file_type):
    """
    Clean and validate data specific to the type of CSV file (e.g., users, streams, movies).
    Args:
        df (DataFrame): The DataFrame to clean.
        file_type (str): Type of the CSV file (e.g., 'users', 'streams', 'movies').
    Returns:
        DataFrame: The cleaned and validated DataFrame.
    """
    df = clean_data(df)
    if file_type == 'users':
        df = df[df['email'].apply(lambda x: re.match(r"[^@]+@[^@]+\.[^@]+", x) is not None)]
    return df

# Function to Remove Special Characters
def remove_special_characters(df):
    """
    Remove special characters like '[', '{', ']', '}', ':', '"', and "'" from the DataFrame.
    Args:
        df (DataFrame): The DataFrame to clean.
    Returns:
        DataFrame: The DataFrame with special characters removed.
    """
    for col in df.columns:
        if df[col].dtype == object:  # Apply only to object type columns (strings)
            # Update the regular expression to ensure proper removal of double quotes
            df[col] = df[col].str.replace('[\[\]{}:"\']', '', regex=True)

    return df


# Load and clean data for each file type
users = load_csv(r'C:/Users/ale/Documents/Strider/data/users.csv')
movies = load_csv(r'C:/Users/ale/Documents/Strider/data/movies.csv')
streams = load_csv(r'C:/Users/ale/Documents/Strider/data/streams.csv')

# Clean the CSV data
users_clean = clean_csv_data(users, 'users')
movies_clean = clean_csv_data(movies, 'movies')
streams_clean = clean_csv_data(streams, 'streams')

# Load, flatten, and clean JSON data
authors = flatten_json(load_json(r'C:/Users/ale/Documents/Strider/data/authors.json'), 'metadata')
books = load_json(r'C:/Users/ale/Documents/Strider/data/books.json')
reviews = flatten_json(load_json(r'C:/Users/ale/Documents/Strider/data/reviews.json'), 'content')

authors_clean = clean_data(authors)
books_clean = clean_data(books)
reviews_clean = clean_data(reviews)

# Remove special characters
authors_clean = remove_special_characters(authors_clean)
books_clean = remove_special_characters(books_clean)
reviews_clean = remove_special_characters(reviews_clean)

# Save cleaned DataFrames
save_partitioned_csv(users_clean, 'users_clean')
save_partitioned_csv(movies_clean, 'movies_clean')
save_partitioned_csv(streams_clean, 'streams_clean')
save_partitioned_csv(authors_clean, 'authors_clean')
save_partitioned_csv(books_clean, 'books_clean')
save_partitioned_csv(reviews_clean, 'reviews_clean')

'''

[Data Provider]
       |
       v
   [AWS S3] --------------------\
       |                        |
       v                        |
[Apache Airflow] --(Orchestrates and processes)--> [ETL Process] --(Transforms)--> [AWS RDS (PostgreSQL)]
       |
       v
[Monitoring and Logs]


'''