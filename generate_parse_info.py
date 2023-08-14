import argparse
import hashlib
import json
from pathlib import Path
from tqdm import tqdm

def get_md5_hash(value:str)->str:
    return hashlib.md5(value.encode()).hexdigest()

def main(args):
    schema_detection_results_filepath:str=args.schema_detection_results_filepath
    processed_root_dirname:str=args.processed_root_dirname
    output_filepath:str=args.output_filepath

    processed_root_dir=Path(processed_root_dirname)

    with open(schema_detection_results_filepath,"r",encoding="utf-8") as r:
        schema_detection_results=json.load(r)

    parse_info_list=[]
    for schema_detection_result in tqdm(schema_detection_results):
        input_filepath=schema_detection_result["filepath"]
        input_file=Path(input_filepath)
        
        input_file_stem=input_file.name.split(".",1)[0]
        processed_dir=processed_root_dir.joinpath(input_file_stem)

        input_filepath_hash=get_md5_hash(input_filepath)
        records_file=processed_dir.joinpath(f"{input_filepath_hash}.tsv")
        parse_errors_file=processed_dir.joinpath(f"{input_filepath_hash}_errors.json")

        parse_info={
            "input_filepath": input_filepath,
            "input_filepath_hash": input_filepath_hash,
            "input_file_stem": input_file_stem,
            "records_filepath": str(records_file),
            "parse_errors_filepath": str(parse_errors_file)
        }

        parse_info_list.append(parse_info)

    with open(output_filepath,"w",encoding="utf-8") as w:
        json.dump(parse_info_list,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-s","--schema-detection-results-filepath",type=str)
    parser.add_argument("-p","--processed-root-dirname",type=str)
    parser.add_argument("-o","--output-filepath",type=str)
    args=parser.parse_args()

    main(args)
