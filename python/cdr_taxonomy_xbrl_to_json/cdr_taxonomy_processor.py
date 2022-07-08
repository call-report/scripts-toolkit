"""
Processes a CDR Taxonomy XBRL file and converts the file to JSON format.

The FFIEC (Federal Financial Institutions Examinations Council)'s Public Data Repository
publishes quarterly taxonomies of the FFIEC 031, 041, and 051 forms in XBRL format, provided
as XBRL files contained within a ZIP file.

Taxonomies include information on the datafields contained in each report, the location
of those data fields within reporting schedules, etc.

Typically these files are processed using commercial software; however, in lieu of that
software, extracting taxonomy information from the XBRL files can be tedious.

This script provides a one-shot process to convert the XBRL files to JSON format.

From the JSON-based files, bulk data may be joined to the CDR Taxonomy JSON files
for purposes of presenting and viewing data in a more analyst-friendly format.

This script is published under the MIT license. Please see the LICENSE file in the
source repository for more information.

TODO: Add documentation for methodology

Version 0.0.2
"""

import sys

# Confirm we hae the correct python version
"""
Checks the python version to ensure we have the correct version.
"""
def check_python_version():
    if sys.version_info < (3, 8):
        print("\nPython version 3.8 or higher is required.\n")
        sys.exit(1)
    return True

_ = check_python_version()

# Loading standard libraries
import os, io, json, collections
import argparse
from zipfile import ZipFile
from itertools import chain

# Before loading extra libraries, check that those libraries are installed
import importlib

lib_networkx = importlib.find_loader('networkx')
lib_xmltodict = importlib.find_loader('xmltodict')
lib_tqdm = importlib.find_loader('tqdm')

missing_lib_networkx = lib_networkx is None
missing_lib_xmltodict = lib_xmltodict is None
missing_lib_tqdm = lib_tqdm is None

if missing_lib_networkx or missing_lib_xmltodict or missing_lib_tqdm:
    print("\nOne or more libraries are missing.\n")
    print("Please install the missing libraries and try again:\n")
    print("missing library: networkx") if missing_lib_networkx else None
    print("missing library: xmltodict") if missing_lib_xmltodict else None
    print("missing library: tqdm") if missing_lib_tqdm else None
    sys.exit(1)

# Load third-party libraries
import networkx as nx
import xmltodict
from tqdm import tqdm


"""
Takes the name of a ZIP file containing and processes the contents of the file.
   
Parameters: 
-----------
filename : str
    The name of the ZIP file to process

"""
def process_zip(filename : str) -> None:
    
    # Loads the zip file included at the command line
    print("Loading ZIP file: ", filename)
    
    raw_data = open(filename,'rb').read()
    zip_io = io.BytesIO()
    zip_io.write(raw_data)
    zip_io.seek(0)
    zip_stream = ZipFile(zip_io)
    files = zip_stream.filelist
    files_parsed = [f.filename for f in files]

    # if any of these statements fail, we likely do not have a CDR taxonomy zip file
    try:
        cap_file = list(filter(lambda x: '-cap' in x, files_parsed))[0]
        def_file = list(filter(lambda x: '-def' in x, files_parsed))[0]
        pres_file = list(filter(lambda x: '-pres' in x, files_parsed))[0]
        ref_file = list(filter(lambda x: '-ref' in x, files_parsed))[0]
        reference_dict = xmltodict.parse(zip_stream.open(ref_file))['linkbase']
        reference_list = reference_dict['referenceLink']['reference']
        reference_arc = reference_dict['referenceLink']['referenceArc']
        cap_dict = xmltodict.parse(zip_stream.open(cap_file))['linkbase']
        def_dict = xmltodict.parse(zip_stream.open(def_file))['linkbase']

    except Exception as e:
        print("The included ZIP file does not appear to be a valid CDR taxonomy file.\n")
        print("Exiting with error")
        sys.exit(1)

    print(filename + " successfully loaded and apparently valid.")

    # this begins the processing of the files
    report_id = dict(cap_dict['roleRef'][0])[
        "@xlink:href"].split(".")[0].replace("call-report", "").split("-")
    form_number = report_id[0]
    report_quarter = "-".join(report_id[1:4])

    ret_pres_dict = []
    pres_dict = json.loads(json.dumps(xmltodict.parse(
        zip_stream.open(pres_file))['linkbase']))['presentationLink']
    for r in pres_dict:
        for rr in r['presentationArc']:
            try:
                ret_pres_dict.append(ret_pres_dict.append(
                    {"from": rr['@xlink:from'], "to": rr['@xlink:to']}))
            except:
                pass

    ret_pres_dict = list(filter(None, ret_pres_dict))

    # get list of froms
    froms = set([r["from"] for r in ret_pres_dict])

    # get list of tos
    tos = set([r["to"] for r in ret_pres_dict])
    end_points = list(tos-froms)
    begin_points = list(froms-tos)
    root = list(begin_points)[0]


    nodes = set()
    edges = []
    # get parent-child relationships
    for r in ret_pres_dict:
        try:
            edges.append((r["to"], r["from"]))
        except:
            pass


    # Begin building the graph object from the source XBRL files
    G = nx.DiGraph()
    G.add_edges_from(edges)

    tree = nx.to_dict_of_dicts(G)
    tree_list = list(tree.items())

    # generate the list...
    endpoints_actual = list(
        filter(lambda x: "cc_" in x or "uc_" in x, end_points))

    # find the path relationships
    ret_paths = [list(nx.all_simple_paths(G, p, root))
                 for p in tqdm(endpoints_actual)]

    cap_labelarc = cap_dict['labelLink']['labelArc']
    cap_labels = cap_dict['labelLink']['label']

    label_dict = {}
    
    # Iterate through the paths to find the labels
    # TODO: replace nested for loops with recursive function
    for r in tqdm(ret_paths):
        for rr in r:
            for rrr in rr:
                for j in cap_labelarc:
                    if j['@xlink:from'] == rrr:
                        for l in cap_labels:
                            if l["@xlink:label"] == j['@xlink:to'] and l["@xlink:label"]:
                                if rrr not in label_dict:
                                    label_dict[rrr] = set()
                                label_dict[rrr] = l["#text"]

    # bring it all toeghter
    # organize the paths into col description and line description
    ret_dict = {}
    for p in ret_paths:

        # remove the root
        new_ret_path = []
        sch_listing = set()
        for pp in p:
            new_ret_path.append(pp[:-1])
            sch_listing.add(pp[-2])

        ret_dict[pp[0]] = {}
        for sch in list(sch_listing):
            new_set = list(filter(lambda x: x[-1] == sch, new_ret_path))
            ret_dict[pp[0]][sch.split("-")[-1]] = {}
            for s in new_set:
                for sr in s:
                    if 'column' in sr:
                        temp_column = list(reversed(s[1:]))
                        ret_col = {}
                        ret_col.update(
                            {"schedule": {"code": temp_column[0], "label": label_dict.get(temp_column[0])}})
                        ret_col.update(
                            {"colset": {"code": temp_column[1], "label": label_dict.get(temp_column[1])}})
                        ret_col.update(
                            {"column": {"code": temp_column[2], "label": label_dict.get(temp_column[2])}})
                        for it, extra_c in enumerate(temp_column[3:]):
                            ret_col.update({f"extra_col_{str(it)}": {
                                           "code": extra_c, "label": label_dict.get(extra_c)}})

                        ret_dict[pp[0]][sch.split(
                            "-")[-1]]["column_ids"] = ret_col
                        break

                    if 'line' in sr:
                        temp_line = list(reversed(s[1:]))
                        ret_line = {}
                        ret_line.update(
                            {"schedule": {"code": temp_line[0], "label": label_dict.get(temp_line[0])}})
                        for it, extra_c in enumerate(temp_line[1:]):
                            ret_line.update(
                                {f"extra_col_{str(it)}": {"code": extra_c, "label": label_dict.get(extra_c)}})

                        ret_dict[pp[0]][sch.split(
                            "-")[-1]]["line_ids"] = ret_line
                        break

    first_dict = {}

    reference_list_sch_key = list(
        filter(lambda x: 'schedule' in x, reference_list[0].keys()))[0]
    reference_list_line_key = list(
        filter(lambda x: 'line' in x, reference_list[0].keys()))[0]
    reference_list_col_key = list(
        filter(lambda x: 'column' in x, reference_list[0].keys()))[0]

    for r in reference_arc:
        if r['@xlink:to'] not in first_dict:
            first_dict[r['@xlink:to']] = {}
        first_dict[r['@xlink:to']].update({r['@xlink:from']: {}})

    first_dict = [{d["@xlink:to"]:[]} for d in reference_arc]

    first_dict = [{'label': d['@xlink:label'], 'schedule':d[reference_list_sch_key],
                   'line':d[reference_list_line_key], "column":d[reference_list_col_key]} for d in reference_list]

    for frow in first_dict:
        label = "_".join(frow["label"].split("_")[0:2])
        if label in ret_dict:
            ret_dict[label][frow['schedule']]['reference'] = {
                "line": frow["line"], "column": frow["column"]}

    final_dict = {"form_number": form_number,
                  "quarter": report_quarter, "data": ret_dict}

    file_name = form_number + "_" + report_quarter + ".json"
    open(file_name, "w").write(json.dumps(final_dict))



"""
Initializes CLI argument parsing
"""
def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [input_file]",
        description="Converts FFIEC XBRL-based CDR file to JSON format",
    )
    parser.add_argument(
        "-v", "--version", action="version",
        version = f"{parser.prog} version 0.0.2"
    )
    parser.add_argument('files', nargs='*')
    return parser


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()

    if len(args.files) == 0:
        print("\nCDR Taxonomy XBRL to JSON Parser")
        print("No files specified. Please specify at least one file.\n")
        return


    for file in args.files:
        print("Processing file: ", file)
        process_zip(file)
        
if __name__ == "__main__":
    main()