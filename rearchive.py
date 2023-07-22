import argparse
import gzip
import json
from pathlib import Path
from tqdm import tqdm

def main(args):
    file_list_filepath:str=args.file_list_filepath
    output_dirname:str=args.output_dirname
    start_index:int=args.start_index

    with open(file_list_filepath,"r",encoding="utf-8") as r:
        file_info_list:list[dict]=json.load(r)

    output_dir=Path(output_dirname)
    output_dir.mkdir(exist_ok=True,parents=True)

    if start_index is None:
        start_index=0

    file_info_list=file_info_list[start_index:]

    for file_info in tqdm(file_info_list):
        input_filepath:str=file_info["filepath"]
        input_filepath_hash:str=file_info["filepath_hash"]

        extension=Path(input_filepath).suffix[1:]
        if extension!="txt":
            raise RuntimeError(f"Unsupported file extension: {extension}")

        with open(input_filepath,"r",encoding="utf-8",errors="replace") as r:
            lines=r.read().splitlines()

        output_file=output_dir.joinpath(f"{input_filepath_hash}.txt.gz")
        with gzip.open(output_file,"wt",encoding="utf-8") as wt:
            for line in lines:
                wt.write(f"{line}\n")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--file-list-filepath",type=str)
    parser.add_argument("-o","--output-dirname",type=str)
    parser.add_argument("-s","--start-index",type=int)
    args=parser.parse_args()
    
    main(args)
