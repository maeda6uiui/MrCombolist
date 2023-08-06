import argparse
import json
from pathlib import Path
from tqdm import tqdm

def main(args):
    input_dirname:str=args.input_dirname
    output_filepath:str=args.output_filepath

    input_dir=Path(input_dirname)
    input_files=list(input_dir.glob("*.json"))

    data_list=[]
    for input_file in tqdm(input_files):
        with input_file.open("r",encoding="utf-8") as r:
            data=json.load(r)
            data_list.append(data)

    with open(output_filepath,"w",encoding="utf-8") as w:
        json.dump(data_list,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    args=parser.parse_args()

    main(args)
