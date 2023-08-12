import argparse
import gzip
import json
import re
from collections import Counter
from pathlib import Path
from tqdm import tqdm

def main(args):
    file_list_filepath:str=args.file_list_filepath
    output_dirname:str=args.output_dirname
    delimiter_candidates:str=args.delimiter_candidates
    start_index:int=args.start_index
    max_num_lines:int=args.max_num_lines
    max_line_length:int=args.max_line_length

    with open(file_list_filepath,"r",encoding="utf-8") as r:
        file_info_list:list[dict]=json.load(r)

    if start_index is not None:
        file_info_list=file_info_list[start_index:]

    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    r_email_head=re.compile(r"^\S+@\S+\.\S+"+f"[{delimiter_candidates}]")
    r_email_tail=re.compile(f"[{delimiter_candidates}]+"+r"\S+@\S+\.\S+$")
    r_email_middle=re.compile(f"[{delimiter_candidates}]+"+r"\S+@\S+\.\S+"+f"[{delimiter_candidates}]")
    r_email_only=re.compile(r"^\S+@\S+\.\S+$")

    for file_info in tqdm(file_info_list):
        input_filepath:str=file_info["filepath"]
        input_filepath_hash:str=file_info["filepath_hash"]

        input_file=Path(input_filepath)
        extension=input_file.suffix[1:].lower()

        fp=None
        if extension=="txt":
            fp=input_file.open("r",encoding="utf-8",errors="ignore")
        elif extension=="gz":
            fp=gzip.open(input_file,"rt",encoding="utf-8",errors="ignore")
        else:
            raise RuntimeError(f"Unsupported extension: {extension}")

        lines=[]
        for idx,line in enumerate(fp):
            if idx>max_num_lines:
                break

            line=line.strip()
            line=line[:max_line_length]
            lines.append(line)

        fp.close()

        possible_placements=[]
        possible_delimiters=[]
        for line in lines:
            possible_placement=""
            possible_delimiter=""

            m_head=r_email_head.search(line)
            m_tail=r_email_tail.search(line)
            m_middle=r_email_middle.search(line)
            m_only=r_email_only.search(line)

            if m_head is not None:
                possible_placement="email:poh"
                possible_delimiter=line[m_head.end()-1]
            elif m_tail is not None:
                possible_placement="poh:email"
                possible_delimiter=line[m_tail.start()]
            elif m_middle is not None:
                possible_placement="unknown:email:unknown"
                possible_delimiter=line[m_middle.start()]
            elif m_only is not None:
                possible_placement="email"
                possible_delimiter="n/a"
            else:
                possible_placement="n/a"
                possible_delimiter="n/a"

            possible_placements.append(possible_placement)
            possible_delimiters.append(possible_delimiter)
        
        placement=""
        delimiter=""
        if len(possible_placements)>0 or len(possible_delimiters)>0:
            c_placements=Counter(possible_placements)
            c_delimiters=Counter(possible_delimiters)

            placement=c_placements.most_common(1)[0][0]
            delimiter=c_delimiters.most_common(1)[0][0]
        else:
            placement="n/a"
            delimiter="n/a"

        schema={
            "placement": placement,
            "delimiter": delimiter
        }
        detection_result={
            "filepath": str(input_file),
            "schema": schema
        }
        
        output_file=output_dir.joinpath(f"{input_filepath_hash}.json")
        with output_file.open("w",encoding="utf-8") as w:
            json.dump(detection_result,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--file-list-filepath",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("-d","--delimiter-candidates",type=str,default=":;|, \t")
    parser.add_argument("-s","--start-index",type=int)
    parser.add_argument("-n","--max-num-lines",type=int,default=1000000)
    parser.add_argument("-l","--max-line-length",type=int,default=200)
    args=parser.parse_args()

    main(args)
