import argparse
import hashlib
import json
from pathlib import Path
from tqdm import tqdm

def get_md5_hash(value:str)->str:
    return hashlib.md5(value.encode()).hexdigest()

def main(args):
    schema_detection_results_filepath:str=args.schema_detection_results_filepath
    output_root_dirname:str=args.output_root_dirname
    output_dirname_type:str=args.output_dirname_type

    if output_dirname_type not in ["parent","stem"]:
        raise RuntimeError(f"Unsupported output dirname type: {output_dirname_type}")

    output_root_dir=Path(output_root_dirname)

    with open(schema_detection_results_filepath,"r",encoding="utf-8") as r:
        schema_detection_results=json.load(r)

    parse_info_list=[]
    for schema_detection_result in tqdm(schema_detection_results):
        input_filepath=schema_detection_result["filepath"]
        input_file=Path(input_filepath)

        output_dir=None
        input_dirname_hash=""
        input_file_stem=""
        if output_dirname_type=="parent":
            input_dirname=input_file.parent.name
            input_dirname_hash=get_md5_hash(input_dirname)
            output_dir=output_root_dir.joinpath(input_dirname_hash)
        elif output_dirname_type=="stem":
            input_file_stem=input_file.name.split(".",1)[0]
            output_dir=output_root_dir.joinpath(input_file_stem)

        input_filepath_hash=get_md5_hash(input_filepath)
        records_file=output_dir.joinpath(f"{input_filepath_hash}.tsv")
        parse_errors_file=output_dir.joinpath(f"{input_filepath_hash}_errors.json")

        parse_info={
            "input_filepath": input_filepath,
            "input_filepath_hash": input_filepath_hash,
            "records_filepath": str(records_file),
            "parse_errors_filepath": str(parse_errors_file)
        }
        if output_dirname_type=="parent":
            parse_info["input_dirname_hash"]=input_dirname_hash
        elif output_dirname_type=="stem":
            parse_info["input_file_stem"]=input_file_stem

        parse_info_list.append(parse_info)

    parse_info_file=output_root_dir.joinpath("parse_info.json")
    with parse_info_file.open("w",encoding="utf-8") as w:
        json.dump(parse_info_list,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--schema-detection-results-filepath",type=str)
    parser.add_argument("-o","--output-root-dirname",type=str)
    parser.add_argument("-t","--output-dirname-type",type=str,default="stem")
    args=parser.parse_args()

    main(args)
