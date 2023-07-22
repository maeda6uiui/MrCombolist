import argparse
import gzip
import json
import re
from collections import Counter
from pathlib import Path
from tqdm import tqdm

def main(args):
    input_root_dirname:str=args.input_root_dirname
    output_filepath:str=args.output_filepath
    delimiter_candidates:str=args.delimiter_candidates

    input_root_dir=Path(input_root_dirname)
    input_files=list(input_root_dir.glob("**/*.txt"))+list(input_root_dir.glob("**/*.txt.gz"))

    r_email_head=re.compile(r"^\S+@\S+\.\S+"+f"[{delimiter_candidates}]")
    r_email_tail=re.compile(f"[{delimiter_candidates}]+"+r"\S+@\S+\.\S+$")
    r_email_middle=re.compile(f"[{delimiter_candidates}]+"+r"\S+@\S+\.\S+"+f"[{delimiter_candidates}]")
    r_email_only=re.compile(r"^\S+@\S+\.\S+$")

    detection_results=[]
    for input_file in tqdm(input_files):
        lines=None
        if input_file.suffix==".txt":
            with input_file.open("r",encoding="utf-8",errors="ignore") as r:
                lines=r.read().splitlines()
        elif input_file.suffix==".gz":
            with gzip.open(input_file,"rt",encoding="utf-8",errors="ignore") as r:
                lines=r.read().splitlines()
        else:
            raise RuntimeError(f"Unsupported suffix: {input_file.suffix}")

        #Remove extra spaces
        lines=[line.strip() for line in lines]

        #Detect placement and delimiter
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
        detection_results.append(detection_result)

    with open(output_filepath,"w",encoding="utf-8") as w:
        json.dump(detection_results,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-root-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("-d","--delimiter-candidates",type=str,default=":;|, \t")
    args=parser.parse_args()

    main(args)
