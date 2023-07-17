import argparse
import json
from tqdm import tqdm

def main(args):
    parse_info_filepath:str=args.parse_info_filepath
    
    with open(parse_info_filepath,"r",encoding="utf-8") as r:
        parse_info_list=json.load(r)

    for parse_info in tqdm(parse_info_list):
        records_filepath:str=parse_info["records_filepath"]

        with open(records_filepath,"r",encoding="utf-8") as r:
            lines=r.read().splitlines()

        cleanup_lines=[]
        
        header=lines[0]
        cleanup_lines.append(f"{header}\n")

        for i in range(1,len(lines)):
            email,poh=lines[i].split("\t",maxsplit=1)
            email=email.strip().replace("\t","")
            poh=poh.replace("\t","")

            cleanup_line=f"{email}\t{poh}\n"
            cleanup_lines.append(cleanup_line)

        with open(records_filepath,"w",encoding="utf-8") as w:
            w.writelines(cleanup_lines)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--parse-info-filepath",type=str)
    args=parser.parse_args()

    main(args)
