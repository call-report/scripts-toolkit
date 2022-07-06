# Call Report Utilities

## CDR XBRL Taxonomy Processor

- Converts zipped CDR Taxonomy Files into a single JSON file. The JSON file contains a heirarchial representation of the relationships described by the XBRL files.

### Requirements

1. Python: Tested on python 3.8.x - 3.10.x

### Installation and Getting Started

1. Clone this repository or copy the `cdr_taxonomy_processor.py` file.
2. Install the required python libraries: `tqdm`, `networkx`, and `xmltodict`
   1. Use pip: `pip install -r requirements.txt`


### Output

- Annotations marked with `#`

```
  "form_number": "031",       # represents ffiec 031, 041, or 051
  "quarter": "2022-06-30", 
  "data": {
    "cc_RCON5570": {          # the internal name of the data field (MDRM)    
      "RCCII": {              # the schedule under which the data field is reported
        "line_ids": {
          "schedule": {
            "code": "ffiec031_pres-RCCII",
            "label": "Schedule RC-C Part II - Loans to Small Businesses and Small Farms(Form Type - 031)"
          },
          "extra_col_0": { 
            "code": "ffiec031_pres-line-967394",
            "label": "Number and amount currently outstanding of \"Commercial and industrial loans to U.S. addressees\" in domestic offices reported in Schedule RC-C, part I, item 4.a, column B:"
          },
          "extra_col_1": {
            "code": "ffiec031_pres-line-966846",
            "label": "With original amounts of $100,000 or less"
          }
        },
        "column_ids": {
          "schedule": {
            "code": "ffiec031_pres-RCCII",
            "label": "Schedule RC-C Part II - Loans to Small Businesses and Small Farms(Form Type - 031)"
          },
          "colset": { "code": "ffiec031_pres-RCCII-25-colset", "label": null },
          "column": {
            "code": "ffiec031_pres-RCCII-25-column-A",
            "label": "(Column A) Number of Loans"
          }
        },
        "reference": { "line": "4a", "column": "A" }
      }
    },
```

### Next Steps

- Incorporate this python utility into a package of Python-based helper utilities to process raw Call report data.
- Enhanced documentation on XBRL processing steps.