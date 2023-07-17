import argparse
import csv
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm

def main(args):
    parse_info_filepath:str=args.parse_info_filepath
    output_dirname:str=args.output_dirname
    num_tsv_per_concat:int=args.num_tsv_per_concat

    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    with open(parse_info_filepath,"r",encoding="utf-8") as r:
        parse_info_list=json.load(r)

    num_input_files=len(parse_info_list)
    num_output_files=num_input_files//num_tsv_per_concat
    if num_input_files%num_tsv_per_concat!=0:
        num_output_files+=1

    for i in tqdm(range(num_output_files)):
        df_concat=None
        for j in range(num_tsv_per_concat):
            parse_info=parse_info_list[i*num_tsv_per_concat+j]
            records_filepath:str=parse_info["records_filepath"]
            input_filepath_hash:str=parse_info["input_filepath_hash"]

            if df_concat is None:
                df_concat=pd.read_table(records_filepath,quoting=csv.QUOTE_NONE)
                df_concat["input_filepath_hash"]=input_filepath_hash
            else:
                df=pd.read_table(records_filepath,quoting=csv.QUOTE_NONE)
                df["input_filepath_hash"]=input_filepath_hash
                df_concat=pd.concat([df_concat,df],axis=0)

        output_file=output_dir.joinpath(f"{i}.tsv")
        df_concat.to_csv(output_file,sep="\t",index=False)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--parse-info-filepath",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("-n","--num-tsv-per-concat",type=int,default=500)
    args=parser.parse_args()

    main(args)
