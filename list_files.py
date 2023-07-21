import argparse
import hashlib
import json
from pathlib import Path
from tqdm import tqdm

def get_md5_hash(value:str)->str:
    return hashlib.md5(value.encode()).hexdigest()

def main(args):
    input_root_dirname:str=args.input_root_dirname
    output_filepath:str=args.output_filepath
    extension:str=args.extension

    pattern=""
    if extension is None:
        pattern="**/*"
    else:
        pattern=f"**/*.{extension}"

    input_root_dir=Path(input_root_dirname)
    input_files=list(input_root_dir.glob(pattern))
    input_files=[x for x in input_files if x.is_file()]

    file_info_list=[]
    for input_file in tqdm(input_files):
        file_info={
            "filepath": str(input_file),
            "filepath_hash": get_md5_hash(str(input_file))
        }
        file_info_list.append(file_info)

    with open(output_filepath,"w",encoding="utf-8") as w:
        json.dump(file_info_list,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-root-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    parser.add_argument("-e","--extension",type=str)
    args=parser.parse_args()

    main(args)
