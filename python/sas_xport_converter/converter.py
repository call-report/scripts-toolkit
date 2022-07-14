"""
converter.py: Converts SAS XPORT format bank regulatory data files from
the Federal Reserve Bank of Chicago

See README.md for requirements, usage, and additional info.
"""

import os
import argparse
import io
from glob import glob
import time

from zipfile import ZipFile
import numpy as np
import json
import pandas as pd
import pyreadstat # see README.md for special instructions on pyreadstat
from uuid import uuid4
from tqdm import tqdm


def bool_detector(series: pd.Series) -> bool:
    """Detects if a data series contained within a pandas dataseries is boolean
       This is needed because booleans are coded into the original data as 1 and 0,
       instead of true/false.
       
       This detection should be accurate, unless a data series contains all zeros,
       or all zeros and one "1".

    Args:
        series (pd.Series): data series to detect

    Returns:
        bool: True if if the data series is boolean, False otherwise
    """
    
    # filter not NaN and drop duplicate vcalues
    series_set = series.loc[series.apply(lambda x: not pd.isnull(x)).tolist()].drop_duplicates()
    
    if len(series_set) > 2:
        # if we have more than two values, then we can't confidently determine if the data series is boolean,
        # since the only possible values are 0 and 1
        return False
    else:
        # check for the edge case where a data series contains only two unique integers
        series_list = sorted(series_set.tolist())
        if series_list == [0.0, 1.0]:
            return True
        else:
            return False


def int_detector(series: pd.Series) -> bool:
    """Detects if a data series contained within a pandas dataseries is integer.
    This is needed because we cannot accurately differentiate between integer and float
    from the original data.
    
    Args:
        series (pd.Series): Data series to detect

    Returns:
        bool: True if the data series is integer, False otherwise
    """
    # sum the series together
    de_duped_series = series.drop_duplicates()
    series_sum = de_duped_series.loc[de_duped_series.apply(lambda x: not pd.isnull(x)).tolist()].sum()
    # if the modulo of the sum is 0, then it likely that the data series comprises integer
    if series_sum % 1 == 0:
        return True
    else:
        # otherwise, it is likely that the data series does not comprise an integer
        return False


def type_detector(series: pd.Series, c: str) -> str:
    
    """The field types presented by the pyreadstat library can be ambiguous.
    This function attempts to determine the type of the data series using a series
    of tests.

    Returns:
        str: field type (bool, int, float, or str)
    """
    
    # if the data series type is a numpy O[bject], we can assume the data type is a string.
    if series.dtype == np.dtype('O'):
        return 'str'
    elif bool_detector(series):
        return 'bool'
    elif int_detector(series):
        return 'int'
    else:
        return 'float'

def sas_xport_file_to_df(filepath: str) -> pd.DataFrame:
    """Converts a SAS XPORT file to a pandas dataframe

    Args:
        blob (bytes): bytes blob of the SAS XPORT file

    Returns:
        pd.DataFrame: pandas dataframe representation of the SAS XPORT file
    """
    
    
    df_list = []
    try:
        # we read the file using the "chunks" method. /most/ files were originally encoded in WINDOWS-1252, but our final output format is UTF-8
        generator = pyreadstat.read_file_in_chunks(pyreadstat.read_xport, filepath, encoding="WINDOWS-1252", chunksize=4000)
        for df, _ in generator:
            df_list.append(df)
        return pd.concat(df_list)

    except Exception as e:
        print("retrying read using latin-1", filepath, " on ", e)
        try:
            # in some cases, the SAS XPORT file was encoded in LATIN1, so that is our fallback if the original SAS XPORT file fails to decode with WINDOWS-1252
            generator = pyreadstat.read_file_in_chunks(pyreadstat.read_xport, filepath, encoding="LATIN1", chunksize=4000)
            for df, _ in generator:
                df_list.append(df)
            return pd.concat(df_list)

        except Exception as ee:
            # if the original SAS XPORT file fails to decode with LATIN1, then we can't decode the file,
            # or the file is corrupt, or there is some other unspecified error
            raise("Unable to process SAS XPORT file. Is the file corrupt? Error: ", ee)


def assemble_output(df_n: pd.DataFrame, type_dict: dict, quarter: str) -> list:
    
    """With the normalized dataframe, type dictionary, and quarter in which the 
    data is reported, this function assembles the output python dictionary
    that is a representation of the original SAS data

    Args:
        df_n (pd.DataFrame): normalized dataframe
        type_dict (dict): dictionary of field types
        quarter: quarter in which the data is reported

    Returns:
        list: python list of dictionaries
    """
    
    # create a list of dictionaries, one for each row in the dataframe
    output_dict_list = []
    
    print("Processing data frame rows....")
    for rssd, data in tqdm(df_n.iterrows(), total=len(df_n)):
        for mdrm, value in data.items():
            new_dict_val = {}
            new_dict_val.update({"rssd": rssd, "mdrm": mdrm, "quarter": quarter})
            if type_dict[mdrm] == 'bool':
                if value == 1 or value == True or value == "true" or value == "True" or value == "TRUE" or value == "1":
                    new_dict_val.update({"bool_data": True, 'data_type': 'bool'})
                else:
                    new_dict_val.update({"bool_data": False, 'data_type': 'bool'})
            elif type_dict[mdrm] == 'int':
                if pd.isnull(value):
                    continue
                else:
                    new_dict_val.update({"int_data": int(value), 'data_type': 'int'})
                    output_dict_list.append(new_dict_val)
            elif type_dict[mdrm] == 'float':
                if pd.isnull(value):
                    continue
                else: 
                    new_dict_val.update({"float_data": float(value), 'data_type': 'float'})
                    output_dict_list.append(new_dict_val)
            elif type_dict[mdrm] == 'str':
                if pd.isnull(value):
                    new_dict_val.update({"str_data": None, 'data_type': 'str'})
                    continue
                else:
                    new_dict_val.update({"str_data": str(value), 'data_type': 'str'})
                    output_dict_list.append(new_dict_val)
            else:
                # this should never happen
                # TODO: raise an error here instead?
                print("UNKNOWN TYPE:", mdrm, value)
            
    return output_dict_list


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    
    """Normalizes the dataframe by aligning column names with the "time series" format used by
       other libraries and code snippets in this repo.

    Args:
        df (pd.DataFrame): dataframe to normalize

    Returns:
        pd.DataFrame: normalized pandas dataframe
    """
    
    normalized_df = df.copy()
    
    ## make column names lowercase
    normalized_df.columns = normalized_df.columns.str.lower()
    
    ## remove the date column
    normalized_df = normalized_df.drop(columns=['date'])
    
    ## rename the entity column to rssd
    normalized_df.rename(columns={'entity': 'rssd'}, inplace=True)
    
    ## set the column rssd as the index
    normalized_df.set_index('rssd', inplace=True)
    
    return normalized_df

    

def process(filename: str) -> str:
    """Takes the name of a zip file or a SAS XPORT file, processes the file,
    and returns a list of dictionaries in JSON format, returned as a string.
    

    Args:
        filename (str): The path of the file to process.

    Returns:
        str: JSON string representation of the list of dictionaries
    """
    
    #  what is the file extension of filename?
    file_ext = filename.split(".")[-1]
    file_ext_lower = file_ext.lower()

    # create empty var for scope
    data = None
    
    # do we have a zip file that contains an XPORT file?
    if file_ext_lower == "zip":    
        # read the filename into zip_io
        zip_io = io.BytesIO(open(filename,'rb').read())
        zip_io.seek(0)
        zip_stream = ZipFile(zip_io)
        files = zip_stream.filelist
    
        file_to_collect = [f.filename for f in files]
        xpt_file_list = list(filter(lambda x: 'xpt' in x or 'XPT' in x, file_to_collect))
        
        if len(xpt_file_list) == 0:
            raise("No SAS Export (xpt) file found in zip file.")    
        else:
            print("Found", len(xpt_file_list), "xpt files in zip file.")
            print("Processing file:", xpt_file_list[0])
            data = zip_stream.open(xpt_file_list[0]).read()
    
    # ...or do we have a SAS XPORT file?
    elif file_ext_lower == "xpt":
        data = open(filename, 'rb').read()
        
    # write out to temp, so we can process
    temp_file_name = f"/tmp/{str(uuid4())}"
    open(temp_file_name,'wb').write(data)
    
    # read the temp file into a pandas dataframe
    df  = sas_xport_file_to_df(temp_file_name)
    
    # for the SAS XPORT files provided by FRB Chicago, the quarter date
    # reported is going to be the same value for each row, so we can take
    # the first value in the date column and use that as the quarter
    
    dt_data = int(df.iloc[0].DATE)
    # normalize the df
    df_n = normalize_df(df)
    print("Detecting column types...")
    type_dict = {c.lower(): type_detector(df[c], c) for c in tqdm(df.columns)}
    output_dict = assemble_output(df_n, type_dict, dt_data)    

    # remove the temp file
    os.unlink(temp_file_name)

    # completed processing, return the output
    print("Completed processing of", filename)
    
    return json.dumps(output_dict)

def init_argparse() -> argparse.ArgumentParser:
    """
    Initializes CLI argument parsing
    """

    # create the CLI parser
    parser = argparse.ArgumentParser(
        usage="%(prog)s",
        description="Processes SAS XPORT files to JSON from the Federal Reserve Bank of Chicago's Commerical Bank Data Archive",
    )
    
    parser.add_argument(
        "-v", action="version", version= f"{parser.prog} version 0.0.1"
    )
    
    # optional output to a file instead of stdout
    parser.add_argument('-o',  help="The file name to use for output. Otherwise, outputs to output.json.", dest="output_file_name")
    
    parser.add_argument('files', nargs='*')
    return parser



def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()


    # how many files are we processing?
    file_proc_length = len(args.files)

    if file_proc_length == 0:
        print("\nXBRL to JSON converter\n")
        print("No files specified. Please specify at least one zip or XPT file.\n")
        return

    
    if file_proc_length > 1:
        print("At this time, only processing one file at a time is supported.\n")

    file_to_process = args.files[0]
    
    unix_epoch_time = str(int(time.time()))
    possible_output_file_name = file_to_process.split(".")[0] + "_" + unix_epoch_time + ".json"
  
    output_str = process(file_to_process)
    
    if args.output_file_name: 
        print("Writing output to file:", args.output_file_name, " note: this step may a minute or two to complete...")
        open(args.output_file_name, 'w').write(output_str)
    else:
        print(f"Writing output to file, {possible_output_file_name}, note: this step may a minute or two to complete...")
        open(possible_output_file_name, 'w').write(output_str)

        
if __name__ == "__main__":
    main()