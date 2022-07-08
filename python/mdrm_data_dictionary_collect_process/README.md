# Call Report Utilities

## MDRM Data Dictionary Collection Process

- Downloads the latest MDRM Data Dictionary ZIP file from the Federal Reserve web site
- "Cleans" the downloaded text - removing non-standard characters, HTML tags, and extra line breaks
- Adds human-readable columns to machine-readable columns

### What does "MDRM" mean?

An MDRM is an acronym-based term that is the Fed's name for a data field.

### Requirements

1. Python: Tested on python 3.8.x - 3.10.x

### Notes

- While this script runs from CLI and produces a JSON file, the code in the script is also useful for purposes of incorporating code elements into other python-based processes.
- The start and end dates represent the first and last dates of the respective data points' active use
  - An end date of 12/31/9999 indicates that the respective data point is in active use

### Installation and Getting Started

1. Clone this repository or copy the `mdrm_data_collect_process.py` file.
2. Install the required python libraries: `pandas`, `numpy`, and `request`
   1. Or use pip: `pip install -r requirements.txt`


### Example Output

```
  {
    "mnemonic": "AAAA",
    "item_code": "FS87",
    "start_date": "9/30/2016 12:00:00 AM",
    "end_date": "12/31/9999 12:00:00 AM",
    "item_name": "ADJUSTMENT FOR INVESTMENTS IN BANKING; FINANCIAL; INSURANCE; AND COMMERCIAL ENTITIES THAT ARE CONSOLIDATED FOR ACCOUNTING PURPOSES BUT OUTSIDE THE SCOPE OF REGULATORY CONSOLIDATION",
    "is_conf": false,
    "item_type": "F",
    "description": null,
    "series_glossary": " Advanced Approaches Regulatory Capital",
    "item_type_explain": "Financial reported",
    "mdrm": "AAAAFS87",
    "reporting_forms": [
      "FFIEC 101"
    ]
  },
```

### Next Steps

- Incorporate this python utility into a package of Python-based helper utilities to process raw Call report data.
- Add additional CLI output options