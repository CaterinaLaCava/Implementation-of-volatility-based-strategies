import os
import pandas as pd
import gzip
import tarfile
import xlrd
import datetime
import matplotlib.pyplot as plt
import numpy as np
import dask
import glob
import re
import vaex

# Load and store data

def get_file_names(folder):
    """
    Function that returns an array of strings, where each string is the name of a file in the folder passed as argument
    Args:
    - folder: path 
    Returns:
    - file_names: array of strings
    """
    file_names = []
    for file in os.listdir(folder):
        file_names.append(file)
    return file_names

 
def create_file(file_name, content):
    """
    Creates a file in the data folder, it takes as argument the name of the file and the content of the file
    Args:
    - file_name: name of a file 
    - content: content of the file
    """
    with open(file_name, 'w') as file:
        file.write(content)


def extract_tar(file_path, output_path):
    """
    Extracts the contents of a .tar file to the specified output path.
    Args:
    - file_path: name of a file 
    - output_path: path where the content of the file will be stored
    """
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
    """
    Transforms xltime into an object datetime
    Args:
    - xltime: float
    """
    date_value = int(xltime)
    time_value = (xltime - date_value) * 24 * 60 * 60  # Convert fraction of a day to seconds
    date_tuple = xlrd.xldate_as_tuple(date_value, 0)  # 0 for 1900-based date system
    year, month, day, hour, minute, second = date_tuple
    date_time_obj = datetime.datetime(year, month, day, hour, minute, second) + datetime.timedelta(seconds=time_value)
    return date_time_obj


def convert_to_float(value):
    """
    Converts the value to float if it is possible, otherwise it returns nan
    Args:
    - value: float
    Returns:
    - float_value: float
    """
    try:
        float_value = float(value)
        return float_value if np.isfinite(float_value) else np.nan
    except (ValueError, TypeError):
        return np.nan
    

def resample_df(df):
    """
    Resample the dataframe df to 1 minute frequency; one apply the function xl_to_datetime to the column xltime of merged_df
    Args:
    - df: dataframe
    Returns:
    -  df: dataframe
    """
    df['datetime'] = df['xltime'].apply(xl_to_datetime)
    df['bid-price'] = df['bid-price'].astype(float)
    df['ask-price'] = df['ask-price'].astype(float)
    df['bid-volume'] = df['bid-volume'].astype(float)
    df['ask-volume'] = df['ask-volume'].astype(float)

    #drop the column xltime
    df = df.drop(columns=['xltime'])

    #set the column datetime as index
    df = df.set_index('datetime')
    df = df.resample('1T').agg({
        'bid-price': 'mean',
        'ask-price': 'mean',
        'bid-volume': 'sum',
        'ask-volume': 'sum'
    })
    return df


def create_folder(directory_path, folder_name):
    """
    Combines directory path and folder name to create the full path for the new folder
    Args:
    - directory_path: path
    - folder_name: name of the folder
    """
    new_folder_path = os.path.join(directory_path, folder_name)

    # Create the new folder if it doesn't already exist
    if not os.path.exists(new_folder_path):
        os.makedirs(new_folder_path)


def clean_dataframe(df):
    """
    Clean the dataframe in input by replacing the non-float values with the previous value
    Args:
    - df: dataframe
    Returns:
    - df: cleaned dataframe
    """
    for column_name in df.columns[1:]:
        # Convert the column to numeric, coercing non-numeric values to NaN
        numeric_column = pd.to_numeric(df[column_name], errors='coerce')
        mean_value = numeric_column.mean()
        if isinstance(df[column_name][0], float)==False:
            df[column_name][0] = mean_value
        for row in range(1, len(df[column_name])):
            if isinstance(df[column_name][row], float)==False:
                df[column_name][row] = df[column_name][row-1]

    return df
        

def clean_dataframe_faster(df):
    for column_name in df.columns[1:]:
        # Convert the column to numeric, coercing non-numeric values to NaN
        numeric_column = pd.to_numeric(df[column_name], errors='coerce')
        mean_value = numeric_column.mean()
        
        # Use vectorized operations to replace non-float values
        non_float_mask = ~pd.api.types.is_float_dtype(df[column_name])
        
        df.loc[non_float_mask, column_name] = df[column_name].shift(1)
        df.loc[0, column_name] = mean_value

    return df


dask.config.set(scheduler="processes")

@dask.delayed
def load_trade(filename,
             tz_exchange="America/New_York",
             only_non_special_trades=True,
             only_regular_trading_hours=True,
             open_time="09:30:00",
             close_time="16:00:00",
             merge_sub_trades=True):
    try:
        if re.search('(csv|csv\\.gz)$',filename):
            DF = pd.read_csv(filename, engine = "pyarrow")
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search('parquet$',filename):
            DF = pd.read_parquet(filename)
    except Exception as e:
     #   print("load_TRTH_trade could not load "+filename)
     #   print(e)
        return None
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None
    if DF.shape[0]==0:
        return None
    if only_non_special_trades:
        DF = DF[DF["trade-stringflag"]=="uncategorized"]
    DF.drop(columns=["trade-rawflag","trade-stringflag"],axis=1,inplace=True)
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    if only_regular_trading_hours:
        DF=DF.between_time(open_time,close_time)    # warning: ever heard e.g. about Thanksgivings?
    if merge_sub_trades:
           DF=DF.groupby(DF.index).agg(trade_price=pd.NamedAgg(column='trade-price', aggfunc='mean'),
                                       trade_volume=pd.NamedAgg(column='trade-volume', aggfunc='sum'))
    return DF


@dask.delayed
def load_bbo(filename,
             tz_exchange="America/New_York",
             only_regular_trading_hours=True,
             merge_sub_trades=True):
    try:
        if re.search(r'(csv|csv\.gz)$',filename):
            DF = pd.read_csv(filename)
        if re.search(r'arrow$',filename):
            DF = pd.read_arrow(filename)
        if re.search(r'parquet$',filename):
            DF = pd.read_parquet(filename) 
    except Exception as e:
       # print("load_TRTH_bbo could not load "+filename)
        return None
    try:
        DF.shape
    except Exception as e: # DF does not exist
        print("DF does not exist")
        print(e)
        return None
    if DF.shape[0]==0:
        return None
    DF.index = pd.to_datetime(DF["xltime"],unit="d",origin="1899-12-30",utc=True)
    DF.index = DF.index.tz_convert(tz_exchange)  # .P stands for Arca, which is based at New York
    DF.drop(columns="xltime",inplace=True)
    if only_regular_trading_hours:
        DF=DF.between_time("09:30:00","16:00:00")    # ever heard about Thanksgivings?
    if merge_sub_trades:
        DF=DF.groupby(DF.index).last()
    return DF


@dask.delayed
def load_merge_trade_bbo(ticker,date,
                         dirBase="data/raw/sp100_2004-8/",
                         suffix="csv.gz",
                         suffix_save=None,
                         dirSaveBase="data/clean/sp100_2004-8/events",
                         saveOnly=False,
                         doSave=False
                        ):
    
    file_trade=dirBase+"/"+"/trade/"+ticker+"/"+str(date.date())+"-"+ticker+"-trade."+suffix
    file_bbo=file_trade.replace("trade","bbo")
    trades=load_trade(file_trade)
    bbos  =load_bbo(file_bbo)
    try:
        trades.shape + bbos.shape
    except:
        return None
    
    events=trades.join(bbos,how="outer")
    
    if doSave:
        dirSave=dirSaveBase+"/"+"/events/"+ticker
        if not os.path.isdir(dirSave):
            os.makedirs(dirSave)

        if suffix_save:
            suffix=suffix_save
        
        file_events=dirSave+"/"+str(date.date())+"-"+ticker+"-events"+"."+suffix
       # pdb.set_trace()

        saved=False
        if suffix=="arrow":
            events=vaex.from_pandas(events,copy_index=True)
            events.export_arrow(file_events)
            saved=True
        if suffix=="parquet":
         #   pdb.set_trace()
            events.to_parquet(file_events,use_deprecated_int96_timestamps=True)
            saved=True
            
        if not saved:
            print("suffix "+suffix+" : format not recognized")
            
        if saveOnly:
            return saved
    return events


def data_to_parquet(ticker):
    if ~os.path.exists(f"data/clean/sp100_2004-8/{ticker}.parquet"):
        trade_files=glob.glob(f"data/raw/sp100_2004-8/trade/{ticker}/*.csv.gz")
        # we have a name for each trading file that we find in the directory; each trading file correspond to one 
        # trading day 
        trade_files.sort()
        allpromises=[load_trade(fn) for fn in trade_files]
        trades=dask.compute(allpromises)[0]
        trades=pd.concat(trades)

        bbo_files=glob.glob(f"data/raw/sp100_2004-8/bbo/{ticker}/*.csv.gz")
        bbo_files.sort()
        allpromises=[load_bbo(fn) for fn in bbo_files]
        bbos=dask.compute(allpromises)[0]
        bbos=pd.concat(bbos)

        events=trades.join(bbos,how="outer")

        # Filling NaNs in 'ask_price' column with the last known value from 'ask_price' column
        events = events.replace('()', np.nan)
        events['ask-price'] = events['ask-price'].bfill()
        events['bid-price'] = events['bid-price'].bfill()
        events['ask-volume'] = events['ask-volume'].bfill()
        events['bid-volume'] = events['bid-volume'].bfill()
        events['ask-price'] = events['ask-price'].ffill()
        events['bid-price'] = events['bid-price'].ffill()
        events['ask-volume'] = events['ask-volume'].ffill()
        events['bid-volume'] = events['bid-volume'].ffill()

        events = events.dropna(subset=['trade_price'])
        events["bid-price"] = events["bid-price"].values.astype("float")
        events["bid-volume"]=events["bid-volume"].values.astype("float")
        events["ask-price"]=events["ask-price"].values.astype("float")
        events["ask-volume"]=events["ask-volume"].values.astype("float")

        events.to_parquet(f"data/clean/sp100_2004-8/{ticker}.parquet")
    

def eigenvalue_clipping(lambdas,v,lambda_plus):
    N=len(lambdas)
    
    
    # _s stands for _structure below
    sum_lambdas_gt_lambda_plus=np.sum(lambdas[lambdas>lambda_plus])
    
    sel_bulk=lambdas<=lambda_plus                     # these eigenvalues come from the seemingly random bulk
    N_bulk=np.sum(sel_bulk)
    sum_lambda_bulk=np.sum(lambdas[sel_bulk])        
    delta=sum_lambda_bulk/N_bulk                      # delta is their average, so as to conserver the trace of C
    
    lambdas_clean=lambdas
    lambdas_clean[lambdas_clean<=lambda_plus]=delta
    
    
    C_clean=np.zeros((N, N))
    v_m=np.matrix(v)
    
    for i in range(N-1):
        C_clean=C_clean+lambdas_clean[i] * np.dot(v_m[i,].T,v_m[i,]) 
        
    np.fill_diagonal(C_clean,1)
            
    return C_clean    
    

def process_parquet_files(ticker, trading_returns):
    if ~os.path.exists(f"data/clean/sp100_2004-8/resampled/{ticker}.parquet"):
        # read the parquet file and resample it (now in the rows there is the time minute by minute)
        df = pd.read_parquet(f"data/clean/sp100_2004-8/")
        df = df.resample('1T').mean()
        # compute returns only in the prices (exclude volumes)
        df_prices = df.drop(columns=['trade_volume', 'bid-volume', 'ask-volume'])
        df_returns = (df_prices / df_prices.shift(1) - 1).dropna() # do we want log returns for some reason? 
        df_returns.to_parquet(f"data/clean/sp100_2004-8/resempled/")
    trading_returns[ticker] = df_returns['trade_price']
