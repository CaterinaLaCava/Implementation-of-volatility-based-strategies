import os
import pandas as pd
import gzip
import tarfile
import xlrd
import datetime
import matplotlib.pyplot as plt
import numpy as np

#function that returns an array of strings, where each string is the name of a file in the folder passed as argument
def get_file_names(folder):
    file_names = []
    for file in os.listdir(folder):
        file_names.append(file)
    return file_names

#define a function that creates a file in the data folder, it takes as argument the name of the file and the content of the file
def create_file(file_name, content):
    with open(file_name, 'w') as file:
        file.write(content)

#this function takes as argument the name of a tar file file_path and uncompress it and store the content in the repertory output_path
def extract_tar(file_path, output_path):
    #Extracts the contents of a .tar file to the specified output path.
    #Args:
    #file_path: The path to the .tar file.
    #output_path: The path where the contents of the .tar file will be extracted.
    try:
        with tarfile.open(file_path, 'r') as tar:
            tar.extractall(output_path)
        print(f"Extraction of {file_path} successful.")
    except tarfile.TarError as e:
        print(f"Error extracting {file_path}: {e}")

def extract_csv_gz_file(source_file, destination_directory):
    """
    Extracts a .csv.gz file to a specified directory.

    Args:
    - source_file: The path to the .csv.gz file to be extracted.
    - destination_directory: The directory where the extracted file will be saved.
    """
    if not os.path.exists(destination_directory):
        os.makedirs(destination_directory)
    try:
        file_name = os.path.basename(source_file)
        output_file = os.path.join(destination_directory, os.path.splitext(file_name)[0])
        with gzip.open(source_file, 'rb') as f_in, open(output_file, 'wb') as f_out:
            f_out.write(f_in.read())
        print(f"File extracted to: {output_file}")
    except Exception as e:
        print(f"Error extracting file: {e}")

def xl_to_datetime(xltime):
    #transform xltime into an object datetime
    date_value = int(xltime)
    time_value = (xltime - date_value) * 24 * 60 * 60  # Convert fraction of a day to seconds
    date_tuple = xlrd.xldate_as_tuple(date_value, 0)  # 0 for 1900-based date system
    year, month, day, hour, minute, second = date_tuple
    date_time_obj = datetime.datetime(year, month, day, hour, minute, second) + datetime.timedelta(seconds=time_value)
    return date_time_obj

def convert_to_float(value):
    #converts the value to float if it is possible, otherwise it returns nan
    try:
        float_value = float(value)
        return float_value if np.isfinite(float_value) else np.nan
    except (ValueError, TypeError):
        return np.nan
    
def resample_df(df):
    #resample the dataframe df to 1 minute frequency
    #one apply the function xl_to_datetime to the column xltime of merged_df
    df['datetime'] = df['xltime'].apply(xl_to_datetime)
    df['bid-price'] = df['bid-price'].astype(float)
    df['ask-price'] = df['ask-price'].astype(float)
    df['bid-volume'] = df['bid-volume'].astype(float)
    df['ask-volume'] = df['ask-volume'].astype(float)
    #one drops the column xltime
    df = df.drop(columns=['xltime'])
    #one sets the column datetime as index
    df = df.set_index('datetime')
    df = df.resample('1T').agg({
        'bid-price': 'mean',
        'ask-price': 'mean',
        'bid-volume': 'sum',
        'ask-volume': 'sum'
    })
    return df

def create_folder(directory_path, folder_name):
    # Combine directory path and folder name to create the full path for the new folder
    new_folder_path = os.path.join(directory_path, folder_name)

    # Create the new folder if it doesn't already exist
    if not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)
