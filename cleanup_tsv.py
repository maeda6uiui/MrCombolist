import argparse
import json
from tqdm import tqdm

def main(args):
    parse_info_filepath:str=args.parse_info_filepath
    progress_output_filepath:str=args.progress_output_filepath
    start_index:int=args.start_index
    
    with open(parse_info_filepath,"r",encoding="utf-8") as r:
        parse_info_list=json.load(r)

    if start_index is not None:
        parse_info_list=parse_info_list[start_index:]

    for idx,parse_info in enumerate(tqdm(parse_info_list)):
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

        #Output cleanup progress to file
        with open(progress_output_filepath,"w",encoding="utf-8") as w:
            progress=0
            if start_index is not None:
                progress=start_index+idx
            else:
                progress=idx

            w.write(f"{progress}\n")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--parse-info-filepath",type=str)
    parser.add_argument("-p","--progress-output-filepath",type=str,default="cleanup_progress.txt")
    parser.add_argument("-s","--start-index",type=int)
    args=parser.parse_args()

    main(args)
