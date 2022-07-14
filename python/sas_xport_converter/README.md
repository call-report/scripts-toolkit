# SAS XPORT Converter

## About

The Federal Reserve Bank of Chicago provides a historical dataset of bank and bank holding company data.

This CLI-based python tool is designed to import and convert data hosted on:
    - https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-1976-2000
    - https://www.chicagofed.org/banking/financial-institution-reports/commercial-bank-data-complete-2001-2010

## Motivation

The bank data is provided in SAS "XPORT" format; this data format is not the easiest to work with:

- Requires either SAS (expensive!) or particular python library to decode
- Boolean data are encoded as integer 1 or 0, instead of boolean
- Integer and float/double data types are not differentiated in the dataset's metadata

## Usage

This python tool parses the incoming XPORT file, identifies the data type of each column using a set of simple heuristics, and converts the incoming data into a standard JSON structure.

Basic usage:
`python converter.py [filename] -o output_filename`

CLI help:
`python converter.py -h`

## Prerequisites

Python:

  - `>= python 3.8.x`

Libraries:
    1. tqdm
    2. numpy
    3. pandas
    4. pyreadstat

### Note on pyreadstat and ARM/M1/M2 architectures

`pyreadstat` can be installed via `pip` on x86 and amd64 platforms. While the library will install via pip, due to compilation of an underlying library to x86, the library will not load. However, the library is compiled properly via conda-forge with the following CLI command: `conda install -c conda-forge pyreadstat`.