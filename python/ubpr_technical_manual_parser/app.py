from PyPDF2 import PdfReader
import requests
import io
from tqdm import tqdm
from copy import deepcopy


url1 = "https://cdr.ffiec.gov/CDRDownload/CDR/UserGuide/v129/FFIEC%20UBPR%20User%20Guide%20Summary%20Ratios--Page%201_2022-07-05.PDF"

data = requests.get(url1).content

# load into an io stream
dataIo = io.BytesIO(data)
dataIo.seek(0)

pdf = fitz.open(stream=dataIo, filetype='pdf')


page_name_font_size = 16.0
page_name_font_name = "Helvetica"
header_footer_font_size = 7.5
item_title_font_size = 14.0
item_title_font_name = "Helvetica-Bold"
item_subtitle_font_size = 10.0
item_subtitle_font_name = "Helvetica-Bold"
item_text_size = 10.0

results = []
for page in tqdm(pdf):
    dict = page.get_text("dict")
    blocks = dict["blocks"]
    for block in blocks:
        if "lines" in block.keys():
            spans = block['lines']
            for span in spans:
                data = span['spans']
                for lines in data:
                    results.append({"text":lines['text'], "size":lines['size'], "font":lines['font']})
                        # lines['text'] -> string, lines['size'] -> font size, lines['font'] -> font name
                    #print(lines['text'])

pdf.close()



last_page_name = ""


cur_record = {
    "is_referenced_concepts": False,
}
cur_record_list = []
is_referenced_concept = False

for index, line in enumerate(results):
    
    if line["text"].strip() == "Referenced Concepts":
        is_referenced_concept = True
        cur_record["is_referenced_concepts"] = True
    
    if line["size"] == 16:
        last_page_name = line["text"]
        cur_record["page_name"] = last_page_name.strip()
    
    if line["size"] == 14 and line["font"] == "Helvetica-Bold":
        last_item_title = line["text"]
        cur_record["title"] = last_item_title.strip()
    
    if line["size"] == 12 and line["font"] == "Helvetica-Bold":
        
        cur_mdrm_line = line["text"].strip()
        cur_mdrm_line_split = cur_mdrm_line.split(" ")
        
        if len(cur_mdrm_line_split) == 2:
            cur_record["item_number"] = cur_mdrm_line_split[0]
            cur_record["mdrm"] = cur_mdrm_line_split[1]
        else:
            cur_record["mdrm"] = cur_mdrm_line
        
        last_item_mdrm = line["text"]
        
        
        cur_record["mdrm"] = last_item_mdrm.strip()
    
    if line["text"] == "NARRATIVE":
        last_narrative_text = ""
        cur_i = index + 1
        while results[cur_i]["text"] != "DESCRIPTION" and results[cur_i]["text"] != "FORMULA" and results[cur_i]["size"] == 10:
            last_narrative_text = last_narrative_text + " " + results[cur_i]["text"]
            cur_i += 1
        cur_record["narrative"] = last_narrative_text.strip()
        

    if line["text"] == "DESCRIPTION":
        last_description_text = ""
        cur_i = index + 1
        while results[cur_i]["text"] != "NARRATIVE" and results[cur_i]["text"] != "FORMULA" and results[cur_i]["size"] == 10:
            last_description_text = last_description_text + " " + results[cur_i]["text"]
            cur_i += 1 
        cur_record["description"] = last_description_text.strip()
            
    if line["text"] == "FORMULA":
        last_formula_text = ""
        cur_i = index + 1
        while results[cur_i]["text"] != "DESCRIPTION" and results[cur_i]["text"] != "NARRATIVE" and results[cur_i]["size"] == 10:
            last_formula_text = last_formula_text + " " + results[cur_i]["text"]
            cur_i += 1 
        cur_record["formula"] = last_formula_text
        cur_record_list.append(deepcopy(cur_record))
        cur_record = {}
        cur_record["page_name"] = last_page_name.strip()
        cur_record["is_referenced_concepts"] = is_referenced_concept


        
    # if line["text"] == "NARRATIVE":
    #     i = 1
    #     while results[i]["text"] != "DESCRIPTION" and results[i]["text"] != "FORMULA" and results[i]["size"] == 10:
    #         last_description_text += results[index + 1]["text"]
    #         index += 1
    # if line["text"] == "FORMULA":
    #     cur_i = index + 1
    #     while results[cur_i]["text"] != "NARRATIVE" and results[cur_i]["text"] != "FORMULA" and results[cur_i]["size"] == 10:
    #         print(results[cur_i]["text"])
    #         last_formula_text += results[cur_i]["text"]
    #         cur_i += 1 
        break
    