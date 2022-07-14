import pyreadstat
from zipfile import ZipFile
import io

from uuid import uuid4
from tqdm import tqdm
import numpy as np
import json
import pandas as pd

import zlib
import os
import argparse
from glob import glob

def bool_detector(series):
    series_set = series.drop_duplicates()
    if len(series_set) > 3:
        return False
    else:
        series_filter = sorted(list(filter(lambda x: not pd.isnull(x), list(series_set.tolist()))))
        if series_filter == [0.0, 1.0]:
            return True
        else:
            return False


def int_detector(series):
    series_sum = series.sum()
    if series_sum % 1 == 0:
        return True
    else:
        return False


def type_detector(series, c):
    is_bool = False
    is_int = False
    is_float = False
    is_str = False

    if series.dtype == np.dtype('O'):
        is_str = True
    else:
        
        uniq_series = series.drop_duplicates()
        # some data have very small floats..
        uniq_series_rounded = series.apply(lambda x: round(x,7) if not pd.isnull(x) else x)

        if len(uniq_series_rounded) < 3:
            bool_filter = sorted(list(filter(lambda x: not pd.isnull(x), list(uniq_series_rounded.tolist()))))
            if bool_filter == [0.0, 1.0]:
                is_bool = True
        else:
            if uniq_series_rounded.sum() % 1 == 0:
                is_int = True
            else:
                is_float = True

    ret = is_bool + is_int + is_float + is_str
    if ret == 0:
        print("none found :", c)
    if ret > 1:
        print("multiple found :", c)
    else:
        if is_bool:
            return "bool"
        elif is_int:
            return "int"
        elif is_float:
            return "float"
        elif is_str:
            return 'str'
    

def file_to_df(cur_file):
    df_list = []
    try:
        generator = pyreadstat.read_file_in_chunks(pyreadstat.read_xport, cur_file, encoding="WINDOWS-1252", chunksize=4000)
        for df, meta in generator:
            df_list.append(df)
        return pd.concat(df_list)

    except Exception as e:
        print("ERROR WITH", cur_file, " on ", e)
        try:
            generator = pyreadstat.read_file_in_chunks(pyreadstat.read_xport, cur_file, encoding="LATIN1", chunksize=4000)
            for df, meta in generator:
                df_list.append(df)
            return pd.concat(df_list)

        except Exception as ee:
            print("ANOTHER ERROR WITH", cur_file, " on ", ee)


    pass

def assemble_output(df_n: pd.DataFrame, type_dict: dict, quarter: str) -> list:
    
    output_dict_list = []
    
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
                print("UNKNOWN TYPE:", mdrm, value)
            
    return output_dict_list


def normalize_df(df):
    
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

    

def process(filename):
    
    #  what is the file extension of filename?
    file_ext = filename.split(".")[-1]
    file_ext_lower = file_ext.lower()

    # create empty var for scope
    data = None
    
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
    elif file_ext_lower == "xpt":
        data = open(filename, 'rb').read()
        
    # write out to temp, so we can process
    temp_file_name = f"/tmp/{str(uuid4())}"
    open(temp_file_name,'wb').write(data)
    

    df  = file_to_df(temp_file_name)
    dt_data = int(df.iloc[0].DATE)
    # normalize the df
    df_n = normalize_df(df)
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

    parser = argparse.ArgumentParser(
        usage="%(prog)s",
        description="Processes SAS XPORT files to JSON from the Federal Reserve Bank of Chicago's Commerical Bank Data Archive",
    )
    parser.add_argument(
        "-v", "--version", action="version", version= f"{parser.prog} version 0.0.2"
    )
    parser.add_argument('files', nargs='*')
    return parser



def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()

    if len(args.files) == 0:
        print("\nXBRL to JSON converter\n")
        print("No files specified. Please specify at least one zip or XPT file.\n")
        return


    for file in args.files:
        print("Processing file: ", file)
        return process(file)
        
if __name__ == "__main__":
    main()