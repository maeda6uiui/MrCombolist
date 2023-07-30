import argparse
import json
import rarfile
import shutil
from pathlib import Path
from tqdm import tqdm

SHUTIL_SUPPORTED_EXTENSIONS=["zip","tar","tar.gz","tar.bz","tar.xz"]
EXTRA_SUPPORTED_EXTENSIONS=["rar"]

def main(args):
    file_list_filepath:str=args.file_list_filepath
    output_root_dirname:str=args.output_root_dirname
    unrar_tool_filepath:str=args.unrar_tool_filepath

    with open(file_list_filepath,"r",encoding="utf-8") as r:
        file_info_list:list[dict]=json.load(r)

    output_root_dir=Path(output_root_dirname)
    output_root_dir.mkdir(exist_ok=True,parents=True)

    rarfile.UNRAR_TOOL=unrar_tool_filepath
    rarfile.PATH_SEP="/"

    unarchive_results=[]
    for file_info in tqdm(file_info_list):
        input_filepath:str=file_info["filepath"]
        input_filepath_hash:str=file_info["filepath_hash"]

        output_dir=output_root_dir.joinpath(input_filepath_hash)
        output_dir.mkdir(exist_ok=True)

        suffixes=Path(input_filepath).suffixes
        if len(suffixes)==0:
            unarchive_result={
                "filepath": input_filepath,
                "filepath_hash": input_filepath_hash,
                "result": "Failure",
                "error": "No suffix in the filepath"
            }
            unarchive_results.append(unarchive_result)

            continue

        extension="".join(suffixes)
        extension=extension[1:].lower()
        if extension not in SHUTIL_SUPPORTED_EXTENSIONS and extension not in EXTRA_SUPPORTED_EXTENSIONS:
            unarchive_result={
                "filepath": input_filepath,
                "filepath_hash": input_filepath_hash,
                "result": "Failure",
                "error": "Unsupported archive extension"
            }
            unarchive_results.append(unarchive_result)

            continue

        try:
            if extension in SHUTIL_SUPPORTED_EXTENSIONS:
                shutil.unpack_archive(input_filepath,output_dir)
            elif extension=="rar":
                with rarfile.RarFile(input_filepath) as rf:
                    rf.extractall(path=output_dir)
        except Exception as e:
            unarchive_result={
                "filepath": input_filepath,
                "filepath_hash": input_filepath_hash,
                "result": "Failure",
                "error": str(e)
            }
            unarchive_results.append(unarchive_result)
        else:
            unarchive_result={
                "filepath": input_filepath,
                "filepath_hash": input_filepath_hash,
                "result": "Success",
                "error": ""
            }
            unarchive_results.append(unarchive_result)

    unarchive_results_file=output_root_dir.joinpath("unarchive_results.json")
    with unarchive_results_file.open("w",encoding="utf-8") as w:
        json.dump(unarchive_results,w,ensure_ascii=False,indent=4)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--file-list-filepath",type=str)
    parser.add_argument("-o","--output-root-dirname",type=str)
    parser.add_argument("--unrar-tool-filepath",type=str,default="./Bin/UnRAR.exe")
    args=parser.parse_args()

    main(args)
