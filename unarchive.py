import argparse
import json
import rarfile
import shutil
import uuid
import yaml
from logging import getLogger,config
from pathlib import Path

SHUTIL_SUPPORTED_EXTENSIONS=["zip","tar","tar.gz","tar.bz","tar.xz"]
UNRAR_SUPPORTED_EXTENSIONS=["rar"]

def str_endswith_any(given:str,candidates:list[str])->bool:
    for candidate in candidates:
        if given.endswith(candidate):
            return True
        
    return False

def main(args):
    input_root_dirname:str=args.input_root_dirname
    output_root_dirname:str=args.output_root_dirname
    unarchive_log_dirname:str=args.unarchive_log_dirname
    start_index:int=args.start_index
    end_index:int=args.end_index
    unrar_tool_filepath:str=args.unrar_tool_filepath

    #Set up logger
    with open("./logging_config.yaml","r",encoding="utf-8") as r:
        logging_config=yaml.safe_load(r)
    
    config.dictConfig(logging_config)

    logger=getLogger(__name__)
    logger.debug(args)

    #Get all files in the input directory
    logger.info("Start searching input files...")

    input_root_dir=Path(input_root_dirname)
    all_files=input_root_dir.glob("**/*")

    #Get list of files that can be unarchived with this script
    input_files:list[Path]=[]
    excluded_files:list[Path]=[]
    supported_extensions=SHUTIL_SUPPORTED_EXTENSIONS+UNRAR_SUPPORTED_EXTENSIONS
    for f in all_files:
        #Check if the filename ends with supported extensions
        if f.is_file():
            if str_endswith_any(f.name.lower(),supported_extensions):
                input_files.append(f)
            else:
                excluded_files.append(f)

    logger.info(f"{len(input_files)} files will be unarchived, and {len(excluded_files)} files are excluded as not supported")

    #Create output directory
    output_root_dir=Path(output_root_dirname)
    output_root_dir.mkdir(exist_ok=True,parents=True)

    #Create a directory for unarchive logs
    unarchive_log_dir=Path(unarchive_log_dirname)
    unarchive_log_dir.mkdir(exist_ok=True,parents=True)

    #Output paths of input files to a text file
    input_filepaths_log_file=unarchive_log_dir.joinpath("input_filepaths.txt")
    with input_filepaths_log_file.open("w",encoding="utf-8") as w:
        for input_file in input_files:
            w.write(f"{str(input_file)}\n")

    #Output paths of excluded files to a text file
    excluded_filepaths_log_file=unarchive_log_dir.joinpath("excluded_filepaths.txt")
    with excluded_filepaths_log_file.open("w",encoding="utf-8") as w:
        for excluded_file in excluded_files:
            w.write(f"{str(excluded_file)}\n")

    #Create a subset of the list if either the start or the end index is specified
    start_index=start_index if start_index is not None else 0
    end_index=end_index if end_index is not None else len(input_files)

    input_files=input_files[start_index:end_index]

    #Set variables for the unrar tool
    rarfile.UNRAR_TOOL=unrar_tool_filepath
    rarfile.PATH_SEP="/"

    #Unarchive the files
    logger.info("Start unarchiving the files...")
    for idx,input_file in enumerate(input_files):
        logger.info(f"Processing '{str(input_file)}' ({idx+1}/{len(input_files)+1})")

        #Create a directory to unarchive the file to
        uuid4_str=str(uuid.uuid4())
        output_dir=output_root_dir.joinpath(uuid4_str)
        output_dir.mkdir(exist_ok=False,parents=False)

        unarchive_result={}
        try:
            #Files that can be unarchived with shutil
            if str_endswith_any(input_file.name.lower(),SHUTIL_SUPPORTED_EXTENSIONS):
                shutil.unpack_archive(input_file,output_dir)
            #Files that can be unarchived with the unrar tool
            elif str_endswith_any(input_file.name.lower(),UNRAR_SUPPORTED_EXTENSIONS):
                with rarfile.RarFile(input_file) as rf:
                    rf.extractall(path=output_dir)
        except Exception as e:
            unarchive_result={
                "filepath": str(input_file),
                "error": True,
                "description": str(e)
            }
            logger.error(f"Failed to unarchive file: '{str(input_file)}'")
        else:
            unarchive_result={
                "filepath": str(input_file),
                "error": False,
                "description": ""
            }

        #Output unarchive result to the log file
        unarchive_log_file=unarchive_log_dir.joinpath(f"{uuid4_str}.json")
        with unarchive_log_file.open("w",encoding="utf-8") as w:
            json.dump(unarchive_result,w,ensure_ascii=False)

    logger.info("Finished unarchiving the files")

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("-i","--input-root-dirname",type=str)
    parser.add_argument("-o","--output-root-dirname",type=str)
    parser.add_argument("-u","--unarchive-log-dirname",type=str)
    parser.add_argument("--start-index",type=int)
    parser.add_argument("--end-index",type=int)
    parser.add_argument("--unrar-tool-filepath",type=str,default="./Bin/UnRAR.exe")
    args=parser.parse_args()

    main(args)
